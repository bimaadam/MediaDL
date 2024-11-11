//! Live checks against PostgreSQL and Redis. They stay ignored so `cargo test`
//! does not need those services. Run them with the compose stack:
//!
//! DATABASE_URL=postgres://media:media-dev-password@127.0.0.1:15432/media \
//! REDIS_URL=redis://127.0.0.1:16379 \
//! cargo test --test live -- --ignored

use std::time::Duration;

use media_engine::adapters::outbound::postgres::PostgresStore;
use media_engine::adapters::outbound::redis::RedisAdapter;
use media_engine::application::{CommitJob, Locks, NewJobDraft, Store};
use media_engine::bootstrap::migrate;
use media_engine::domain::{
    Actor, ContentId, DeliveryChannel, MediaIdentity, PlatformKind, ProcessingProfile, UserKind,
};
use uuid::Uuid;

fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

#[tokio::test]
#[ignore = "requires DATABASE_URL and a migrated Postgres"]
async fn postgres_keeps_one_active_job_for_one_profile() {
    if let Err(error) = postgres_keeps_one_active_job().await {
        panic!("{error}");
    }
}

async fn postgres_keeps_one_active_job() -> Result<(), String> {
    let store = PostgresStore::connect(&required("DATABASE_URL")?, 2)
        .await
        .map_err(|error| error.to_string())?;
    migrate(store.pool()).await.map_err(|error| error.to_string())?;
    let profile = ProcessingProfile::video_mp4(720).map_err(|error| error.to_string())?;
    let content = format!("live{}", &Uuid::now_v7().simple().to_string()[..8]);
    let identity = MediaIdentity::derive(
        PlatformKind::YouTube,
        ContentId::new(content.clone()).map_err(|error| error.to_string())?,
        &profile,
    )
    .map_err(|error| error.to_string())?;
    let first_user = store
        .upsert_external_user(UserKind::Api, &format!("live-a-{}", Uuid::now_v7()), None)
        .await
        .map_err(|error| error.to_string())?;
    let second_user = store
        .upsert_external_user(UserKind::Api, &format!("live-b-{}", Uuid::now_v7()), None)
        .await
        .map_err(|error| error.to_string())?;
    let created = store
        .commit_new_job(draft(Uuid::now_v7(), &identity, first_user.id, &profile))
        .await
        .map_err(|error| error.to_string())?;
    let attached = store
        .commit_new_job(draft(Uuid::now_v7(), &identity, second_user.id, &profile))
        .await
        .map_err(|error| error.to_string())?;
    let CommitJob::Created { job_id, media_id } = created else {
        return Err("the first insert must create the job".into());
    };
    match attached {
        CommitJob::Attached { job_id: attached_id, media_id: attached_media } => {
            if attached_id != job_id || attached_media != media_id {
                return Err("the second insert must attach to the same job".into());
            }
        }
        CommitJob::Created { .. } => return Err("the second insert created a duplicate job".into()),
    }
    let _ = content;
    Ok(())
}

fn draft(
    job_id: Uuid,
    identity: &MediaIdentity,
    user_id: Uuid,
    profile: &ProcessingProfile,
) -> NewJobDraft {
    NewJobDraft {
        job_id,
        platform: identity.platform(),
        content_id: identity.content_id().as_str().to_owned(),
        canonical_url: format!(
            "https://www.youtube.com/watch?v={}",
            identity.content_id().as_str()
        ),
        profile: *profile,
        profile_hash: identity.profile_hash().as_str().to_owned(),
        actor: Actor { user_id, kind: UserKind::Api },
        channel: DeliveryChannel::Http,
        max_attempts: 5,
    }
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn redis_lock_is_exclusive_to_its_owner() {
    if let Err(error) = redis_lock().await {
        panic!("{error}");
    }
}

async fn redis_lock() -> Result<(), String> {
    let redis = RedisAdapter::connect(
        &required("REDIS_URL")?,
        "media:jobs:test",
        "live",
        "media:jobs:test:dead",
    )
    .await
    .map_err(|error| error.to_string())?;
    let key = format!("lock:test:{}", Uuid::now_v7());
    let first = Locks::try_acquire(&redis, &key, "owner-a", Duration::from_secs(5))
        .await
        .map_err(|error| error.to_string())?;
    let second = Locks::try_acquire(&redis, &key, "owner-b", Duration::from_secs(5))
        .await
        .map_err(|error| error.to_string())?;
    let released =
        Locks::release(&redis, &key, "owner-b").await.map_err(|error| error.to_string())?;
    let released_by_owner =
        Locks::release(&redis, &key, "owner-a").await.map_err(|error| error.to_string())?;
    if !first || second || released || !released_by_owner {
        return Err(format!(
            "lock results first={first} second={second} foreign_release={released} owner_release={released_by_owner}"
        ));
    }
    Ok(())
}
