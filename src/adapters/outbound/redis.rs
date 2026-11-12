//! Redis cache, locks, rate limits, and the job stream.
//!
//! Locks are acquired with SET NX PX and renewed or released only when the
//! stored owner token matches. Stream entries stay pending until a worker acks
//! a terminal outcome.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::streams::{StreamAutoClaimOptions, StreamReadOptions};
use redis::{AsyncCommands, Script, SetExpiry, SetOptions};
use uuid::Uuid;

use crate::application::{
    Admission, AppError, Cache, FormatSelection, JobProgress, JobQueue, Locks, MediaMetadata,
    QueueDelivery, QueueMessage, TelegramFileRef,
};
use crate::domain::{CachedArtifact, MediaIdentity, PlatformKind, metadata_cache_key};

const ACQUIRE: &str = "return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])";
const RENEW: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"#;
const RELEASE: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;
const ADMIT: &str = r#"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return count
"#;

#[derive(Clone)]
pub struct RedisAdapter {
    manager: ConnectionManager,
    stream: String,
    group: String,
    dead_stream: String,
}

impl RedisAdapter {
    pub async fn connect(
        url: &str,
        stream: &str,
        group: &str,
        dead_stream: &str,
    ) -> Result<Self, AppError> {
        let client = redis::Client::open(url)
            .map_err(|error| AppError::Dependency(format!("redis url: {error}")))?;
        // redis-rs defaults the response timeout to 500ms. XREAD BLOCK is 2s, so an
        // idle read is reported as a timeout and stalls the shared connection.
        let manager = ConnectionManager::new_with_config(
            client,
            redis::aio::ConnectionManagerConfig::new()
                .set_response_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .map_err(|error| AppError::Dependency(format!("redis connect: {error}")))?;
        let adapter = Self {
            manager,
            stream: stream.to_owned(),
            group: group.to_owned(),
            dead_stream: dead_stream.to_owned(),
        };
        adapter.ensure_group().await?;
        Ok(adapter)
    }

    fn connection(&self) -> ConnectionManager {
        self.manager.clone()
    }

    async fn ensure_group(&self) -> Result<(), AppError> {
        let mut conn = self.connection();
        match conn.xgroup_create_mkstream::<_, _, _, ()>(&self.stream, &self.group, "$").await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
            Err(error) => Err(AppError::Dependency(format!("redis group: {error}"))),
        }
    }
}

fn redis_err(error: redis::RedisError) -> AppError {
    AppError::Dependency(format!("redis: {error}"))
}

fn ttl_millis(ttl: Duration) -> u64 {
    ttl.as_millis().max(1) as u64
}

#[async_trait]
impl Locks for RedisAdapter {
    async fn try_acquire(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut conn = self.connection();
        let acquired: Option<String> = Script::new(ACQUIRE)
            .key(key)
            .arg(owner)
            .arg(ttl_millis(ttl))
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(acquired.as_deref() == Some("OK"))
    }

    async fn renew(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut conn = self.connection();
        let renewed: i64 = Script::new(RENEW)
            .key(key)
            .arg(owner)
            .arg(ttl_millis(ttl))
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(renewed == 1)
    }

    async fn release(&self, key: &str, owner: &str) -> Result<bool, AppError> {
        let mut conn = self.connection();
        let removed: i64 = Script::new(RELEASE)
            .key(key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(removed == 1)
    }
}

#[async_trait]
impl Cache for RedisAdapter {
    async fn get_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<CachedArtifact>, AppError> {
        self.get_json(&identity.cache_key()).await
    }

    async fn put_artifact(
        &self,
        identity: &MediaIdentity,
        artifact: &CachedArtifact,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.put_json(&identity.cache_key(), artifact, ttl).await
    }

    async fn invalidate_artifact(&self, identity: &MediaIdentity) -> Result<(), AppError> {
        self.delete_key(&identity.cache_key()).await
    }

    async fn get_metadata(
        &self,
        platform: PlatformKind,
        content_id: &str,
    ) -> Result<Option<MediaMetadata>, AppError> {
        self.get_json(&metadata_cache_key(platform, content_id)).await
    }

    async fn put_metadata(
        &self,
        platform: PlatformKind,
        content_id: &str,
        meta: &MediaMetadata,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.put_json(&metadata_cache_key(platform, content_id), meta, ttl).await
    }

    async fn get_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<Option<TelegramFileRef>, AppError> {
        self.get_json(&identity.telegram_key(bot_id)).await
    }

    async fn put_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
        file: &TelegramFileRef,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.put_json(&identity.telegram_key(bot_id), file, ttl).await
    }

    async fn invalidate_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<(), AppError> {
        self.delete_key(&identity.telegram_key(bot_id)).await
    }

    async fn put_progress(
        &self,
        job_id: Uuid,
        progress: &JobProgress,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.put_json(&format!("progress:{job_id}"), progress, ttl).await
    }

    async fn get_progress(&self, job_id: Uuid) -> Result<Option<JobProgress>, AppError> {
        self.get_json(&format!("progress:{job_id}")).await
    }

    async fn put_selection(
        &self,
        token: &str,
        selection: &FormatSelection,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.put_json(&format!("sel:{token}"), selection, ttl).await
    }

    async fn get_selection(&self, token: &str) -> Result<Option<FormatSelection>, AppError> {
        self.get_json(&format!("sel:{token}")).await
    }

    async fn ping(&self) -> Result<(), AppError> {
        let mut conn = self.connection();
        let pong: String = redis::cmd("PING").query_async(&mut conn).await.map_err(redis_err)?;
        if pong == "PONG" { Ok(()) } else { Err(AppError::Dependency("redis ping failed".into())) }
    }
}

impl RedisAdapter {
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, AppError> {
        let mut conn = self.connection();
        let raw: Option<String> = conn.get(key).await.map_err(redis_err)?;
        match raw {
            None => Ok(None),
            Some(raw) => serde_json::from_str(&raw)
                .map(Some)
                .or_else(|_| {
                    metrics::counter!("media_engine_cache_lookups_total", "result" => "stale", "kind" => "decode").increment(1);
                    Ok(None)
                }),
        }
    }

    async fn put_json<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let body = serde_json::to_string(value)
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        let mut conn = self.connection();
        let options = SetOptions::default().with_expiration(SetExpiry::PX(ttl_millis(ttl)));
        let _: Option<String> = conn.set_options(key, body, options).await.map_err(redis_err)?;
        Ok(())
    }

    async fn delete_key(&self, key: &str) -> Result<(), AppError> {
        let mut conn = self.connection();
        let _: i64 = conn.del(key).await.map_err(redis_err)?;
        Ok(())
    }
}

#[async_trait]
impl JobQueue for RedisAdapter {
    async fn publish(&self, message: QueueMessage) -> Result<(), AppError> {
        let mut conn = self.connection();
        let _: Option<String> = conn
            .xadd(&self.stream, "*", &[("job_id", message.job_id.to_string())])
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn receive(
        &self,
        consumer: &str,
        max: usize,
        block: Duration,
    ) -> Result<Vec<QueueDelivery>, AppError> {
        self.read_group(consumer, max, block).await
    }

    async fn ack(&self, delivery_id: &str) -> Result<(), AppError> {
        let mut conn = self.connection();
        let _: usize =
            conn.xack(&self.stream, &self.group, &[delivery_id]).await.map_err(redis_err)?;
        Ok(())
    }

    async fn dead_letter(&self, message: QueueMessage, reason: &str) -> Result<(), AppError> {
        let mut conn = self.connection();
        let _: Option<String> = conn
            .xadd(
                &self.dead_stream,
                "*",
                &[("job_id", message.job_id.to_string()), ("reason", reason.to_owned())],
            )
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn depth(&self) -> Result<u64, AppError> {
        let mut conn = self.connection();
        let len: usize = conn.xlen(&self.stream).await.map_err(redis_err)?;
        metrics::gauge!("media_engine_queue_depth").set(len as f64);
        Ok(len as u64)
    }

    async fn reclaim_stale(
        &self,
        consumer: &str,
        min_idle: Duration,
        max: usize,
    ) -> Result<Vec<QueueDelivery>, AppError> {
        let mut conn = self.connection();
        let reply: redis::streams::StreamAutoClaimReply = conn
            .xautoclaim_options(
                &self.stream,
                &self.group,
                consumer,
                min_idle.as_millis().max(1) as usize,
                "0-0",
                StreamAutoClaimOptions::default().count(max),
            )
            .await
            .map_err(redis_err)?;
        Ok(deliveries_from_ids(reply.claimed))
    }
}

impl RedisAdapter {
    async fn read_group(
        &self,
        consumer: &str,
        max: usize,
        block: Duration,
    ) -> Result<Vec<QueueDelivery>, AppError> {
        let mut conn = self.connection();
        let options = StreamReadOptions::default()
            .group(&self.group, consumer)
            .count(max)
            .block(block.as_millis().min(usize::MAX as u128) as usize);
        let reply: Option<redis::streams::StreamReadReply> =
            conn.xread_options(&[&self.stream], &[">"], &options).await.map_err(redis_err)?;
        let Some(reply) = reply else {
            return Ok(Vec::new());
        };
        let mut deliveries = Vec::new();
        for key in reply.keys {
            deliveries.extend(deliveries_from_ids(key.ids));
        }
        Ok(deliveries)
    }
}

fn deliveries_from_ids(ids: Vec<redis::streams::StreamId>) -> Vec<QueueDelivery> {
    ids.into_iter()
        .filter_map(|entry| {
            let job_id = entry.get::<String>("job_id")?;
            let job_id = Uuid::parse_str(&job_id).ok()?;
            Some(QueueDelivery { id: entry.id, message: QueueMessage { job_id } })
        })
        .collect()
}

#[async_trait]
impl Admission for RedisAdapter {
    async fn allow(
        &self,
        bucket: &str,
        subject: &str,
        limit: u32,
        window: Duration,
    ) -> Result<bool, AppError> {
        let key = if bucket == "quota" {
            let day = chrono::Utc::now().format("%Y-%m-%d");
            format!("quota:{subject}:{day}")
        } else {
            format!("rate:{bucket}:{subject}")
        };
        let mut conn = self.connection();
        let count: i64 = Script::new(ADMIT)
            .key(key)
            .arg(ttl_millis(window))
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(count <= i64::from(limit))
    }
}
