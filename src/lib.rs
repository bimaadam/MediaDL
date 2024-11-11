//! Multi-platform media engine.
//!
//! Delivery adapters sit outside the domain. Shared ownership (`Arc`) is limited
//! to the composition root, where trait objects are shared by the API and workers.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod adapters;
pub mod application;
pub mod bootstrap;
pub mod domain;
pub mod infrastructure;
pub mod shared;
pub mod workers;

#[cfg(test)]
mod testkit;

#[cfg(test)]
mod dedup_tests {
    use std::sync::Arc;

    use tokio::task::JoinSet;
    use uuid::Uuid;

    use crate::application::SubmitCommand;
    use crate::domain::{DeliveryChannel, ProcessingProfile, UserKind};
    use crate::testkit::{self, actor};

    fn command(user: Uuid) -> SubmitCommand {
        SubmitCommand {
            url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ&utm_source=share&t=10".into(),
            profile: ProcessingProfile::video_mp4(720).expect("profile"),
            actor: actor(user),
            channel: DeliveryChannel::Http,
        }
    }

    #[tokio::test]
    async fn one_hundred_concurrent_requests_create_one_job() {
        let (engine, store) = testkit::engine_parts(true, false);
        let mut tasks = JoinSet::new();
        for index in 0..100 {
            let engine = Arc::clone(&engine);
            tasks.spawn(async move {
                let mut cmd = command(Uuid::now_v7());
                if index % 2 == 0 {
                    cmd.url = "https://youtu.be/dQw4w9WgXcQ?si=tracking".into();
                }
                engine.submit(cmd).await
            });
        }
        let mut queued = 0;
        while let Some(joined) = tasks.join_next().await {
            let outcome = joined.expect("task").expect("submit");
            match outcome {
                crate::application::SubmitOutcome::Queued { attached, .. } => {
                    queued += 1;
                    let _ = attached;
                }
                other => panic!("unexpected outcome {other:?}"),
            }
        }
        assert_eq!(queued, 100);
        assert_eq!(store.commits().await, 1, "one extraction job");
        assert_eq!(store.job_count().await, 1);
        assert_eq!(store.subscription_count().await, 100);
    }

    #[tokio::test]
    async fn broken_lock_still_dedups_through_the_store_constraint() {
        let (engine, store) = testkit::engine_parts(false, true);
        let mut tasks = JoinSet::new();
        for _ in 0..40 {
            let engine = Arc::clone(&engine);
            tasks.spawn(async move { engine.submit(command(Uuid::now_v7())).await });
        }
        while let Some(joined) = tasks.join_next().await {
            joined.expect("task").expect("submit");
        }
        assert_eq!(store.commits().await, 1);
        assert_eq!(store.subscription_count().await, 40);
    }

    #[tokio::test]
    async fn different_profiles_are_not_deduplicated() {
        let (engine, store) = testkit::engine_parts(false, false);
        let user = Uuid::now_v7();
        engine.submit(command(user)).await.expect("720");
        let mut audio = command(user);
        audio.profile = ProcessingProfile::audio_mp3();
        audio.actor.kind = UserKind::Api;
        engine.submit(audio).await.expect("audio");
        assert_eq!(store.commits().await, 2);
    }
}
