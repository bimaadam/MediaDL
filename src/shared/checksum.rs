use std::path::Path;

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::application::AppError;

/// Stream a file through SHA-256. The file is never loaded whole into memory.
pub async fn sha256_file(path: &Path) -> Result<(String, u64), AppError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| AppError::Dependency(format!("open output: {error}")))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buf)
            .await
            .map_err(|error| AppError::Dependency(format!("read output: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        total += read as u64;
    }
    Ok((hex::encode(hasher.finalize()), total))
}
