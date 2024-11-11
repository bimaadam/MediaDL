//! S3-compatible artifact storage. Uploads stream from disk. Multipart parts
//! are 8 MiB, so a large file is never held whole in memory.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use s3::{AddressingStyle, Auth, Client, Credentials};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::ReaderStream;

use crate::application::{AppError, ObjectBody, ObjectStorage};

const PART_BYTES: u64 = 8 * 1024 * 1024;

pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self, AppError> {
        let credentials = Credentials::new(access_key, secret_key)
            .map_err(|error| AppError::Dependency(format!("storage credentials: {error}")))?;
        let client = Client::builder(endpoint)
            .map_err(|error| AppError::Dependency(format!("storage endpoint: {error}")))?
            .region(region)
            .auth(Auth::Static(credentials))
            .addressing_style(AddressingStyle::Path)
            .build()
            .map_err(|error| AppError::Dependency(format!("storage client: {error}")))?;
        Ok(Self { client, bucket: bucket.to_owned() })
    }

    /// Confirms the bucket is reachable. Creates it only when it is missing, so an
    /// object-scoped token for an existing bucket does not need CreateBucket.
    pub async fn ensure_bucket(&self) -> Result<(), AppError> {
        match self.client.buckets().head(&self.bucket).send().await {
            Ok(_) => return Ok(()),
            Err(error) if missing(&error) => {}
            Err(error) => return Err(storage_err(error)),
        }
        match self.client.buckets().create(&self.bucket).send().await {
            Ok(_) => Ok(()),
            Err(error) if bucket_exists(&error) => Ok(()),
            Err(error) => Err(storage_err(error)),
        }
    }
}

fn bucket_exists(error: &s3::Error) -> bool {
    match error {
        s3::Error::Api(api) => {
            matches!(api.code.as_deref(), Some("BucketAlreadyOwnedByYou" | "BucketAlreadyExists"))
        }
        _ => false,
    }
}

fn missing(error: &s3::Error) -> bool {
    match error {
        s3::Error::Api(api) => {
            api.status == StatusCode::NOT_FOUND
                || matches!(api.code.as_deref(), Some("NoSuchKey" | "NotFound" | "NoSuchBucket"))
        }
        _ => false,
    }
}

fn storage_err(error: s3::Error) -> AppError {
    AppError::Dependency(format!("object storage: {error}"))
}

struct UnpinReader {
    inner: Pin<Box<dyn AsyncRead + Send>>,
}

impl AsyncRead for UnpinReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.get_mut().inner.as_mut().poll_read(cx, buf)
    }
}

#[async_trait]
impl ObjectStorage for S3Storage {
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        content_type: &str,
        checksum_hex: &str,
        multipart_threshold: u64,
    ) -> Result<(), AppError> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| AppError::Dependency(format!("open artifact: {error}")))?;
        let length = file
            .metadata()
            .await
            .map_err(|error| AppError::Dependency(format!("stat artifact: {error}")))?
            .len();
        if length >= multipart_threshold {
            self.upload_multipart(key, file, length, content_type, checksum_hex).await
        } else {
            let stream = ReaderStream::new(file);
            self.client
                .objects()
                .put(&self.bucket, key)
                .content_type(content_type)
                .map_err(|error| AppError::Dependency(error.to_string()))?
                .metadata("sha256", checksum_hex)
                .map_err(|error| AppError::Dependency(error.to_string()))?
                .body_stream_sized(stream, length)
                .send()
                .await
                .map_err(storage_err)?;
            Ok(())
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, AppError> {
        match self.client.objects().head(&self.bucket, key).send().await {
            Ok(_) => Ok(true),
            Err(error) if missing(&error) => Ok(false),
            Err(error) => Err(storage_err(error)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), AppError> {
        match self.client.objects().delete(&self.bucket, key).send().await {
            Ok(_) => Ok(()),
            Err(error) if missing(&error) => Ok(()),
            Err(error) => Err(storage_err(error)),
        }
    }

    async fn open(&self, key: &str) -> Result<ObjectBody, AppError> {
        let object =
            self.client.objects().get(&self.bucket, key).send().await.map_err(storage_err)?;
        Ok(ObjectBody {
            content_type: object
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_owned()),
            content_length: object.content_length,
            reader: Box::new(UnpinReader { inner: object.body }),
        })
    }

    async fn ping(&self) -> Result<(), AppError> {
        self.client.buckets().head(&self.bucket).send().await.map_err(storage_err)?;
        Ok(())
    }
}

impl S3Storage {
    async fn upload_multipart(
        &self,
        key: &str,
        mut file: tokio::fs::File,
        length: u64,
        content_type: &str,
        checksum_hex: &str,
    ) -> Result<(), AppError> {
        let created = self
            .client
            .objects()
            .create_multipart_upload(&self.bucket, key)
            .content_type(content_type)
            .map_err(|error| AppError::Dependency(error.to_string()))?
            .send()
            .await
            .map_err(storage_err)?;
        let upload_id = created.upload_id;
        let parts = match self.upload_parts(key, &upload_id, &mut file, length).await {
            Ok(parts) => parts,
            Err(error) => {
                let _ = self
                    .client
                    .objects()
                    .abort_multipart_upload(&self.bucket, key, &upload_id)
                    .send()
                    .await;
                return Err(error);
            }
        };
        self.client
            .objects()
            .complete_multipart_upload(&self.bucket, key, &upload_id)
            .parts(parts)
            .map_err(|error| AppError::Dependency(error.to_string()))?
            .send()
            .await
            .map_err(storage_err)?;
        tracing::debug!(checksum = checksum_hex, bytes = length, "multipart upload completed");
        Ok(())
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        file: &mut tokio::fs::File,
        length: u64,
    ) -> Result<Vec<s3::types::CompletedPart>, AppError> {
        let mut parts = Vec::new();
        let mut remaining = length;
        let mut part_number = 1_u32;
        while remaining > 0 {
            let take = remaining.min(PART_BYTES) as usize;
            let mut buffer = vec![0_u8; take];
            file.read_exact(&mut buffer)
                .await
                .map_err(|error| AppError::Dependency(format!("read part: {error}")))?;
            let uploaded = self
                .client
                .objects()
                .upload_part(&self.bucket, key, upload_id, part_number)
                .body_bytes(Bytes::from(buffer))
                .send()
                .await
                .map_err(storage_err)?;
            let etag = uploaded
                .etag
                .ok_or_else(|| AppError::Dependency("part upload omitted an etag".into()))?;
            parts.push(
                s3::types::CompletedPart::new(part_number, etag)
                    .map_err(|error| AppError::Dependency(error.to_string()))?,
            );
            remaining -= take as u64;
            part_number = part_number.saturating_add(1);
        }
        Ok(parts)
    }
}
