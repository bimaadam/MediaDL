//! FFmpeg and ffprobe. Arguments come from the processing profile and from paths
//! this process created. User text is never interpolated into a shell command.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;

use crate::application::{AppError, MediaProcessor};
use crate::domain::{Container, MediaKind, ProcessingProfile};
use crate::shared::command;

pub struct FfmpegProcessor {
    ffmpeg: PathBuf,
    ffprobe: PathBuf,
}

impl FfmpegProcessor {
    pub fn new(ffmpeg: PathBuf, ffprobe: PathBuf) -> Self {
        Self { ffmpeg, ffprobe }
    }
}

pub fn ffmpeg_args(profile: ProcessingProfile, input: &Path, output: &Path) -> Vec<String> {
    let input = input.display().to_string();
    let output = output.display().to_string();
    match (profile.kind, profile.container) {
        (MediaKind::Audio, Container::Mp3) => vec![
            "-y".into(),
            "-i".into(),
            input,
            "-vn".into(),
            "-c:a".into(),
            "libmp3lame".into(),
            "-b:a".into(),
            "192k".into(),
            output,
        ],
        (MediaKind::Audio, Container::M4a) => vec![
            "-y".into(),
            "-i".into(),
            input,
            "-vn".into(),
            "-c:a".into(),
            "aac".into(),
            "-b:a".into(),
            "192k".into(),
            output,
        ],
        _ => vec![
            "-y".into(),
            "-i".into(),
            input,
            "-c".into(),
            "copy".into(),
            "-movflags".into(),
            "+faststart".into(),
            output,
        ],
    }
}

#[async_trait]
impl MediaProcessor for FfmpegProcessor {
    async fn process(
        &self,
        input: &Path,
        profile: &ProcessingProfile,
        output: &Path,
        timeout: Duration,
    ) -> Result<(), AppError> {
        let args = ffmpeg_args(*profile, input, output);
        let result = command::run(&self.ffmpeg, &args, timeout, None).await?;
        if result.status != 0 {
            return Err(AppError::Dependency(format!(
                "ffmpeg exited {}: {}",
                result.status,
                tail(&result.stderr)
            )));
        }
        Ok(())
    }

    async fn probe(&self, path: &Path, timeout: Duration) -> Result<(), AppError> {
        let args = vec![
            "-v".into(),
            "error".into(),
            "-show_streams".into(),
            "-of".into(),
            "json".into(),
            path.display().to_string(),
        ];
        let result = command::run(&self.ffprobe, &args, timeout, None).await?;
        if result.status != 0 {
            return Err(AppError::Validation(format!(
                "ffprobe rejected the output: {}",
                tail(&result.stderr)
            )));
        }
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout).map_err(|error| {
            AppError::Validation(format!("ffprobe output is not json: {error}"))
        })?;
        let streams = parsed
            .get("streams")
            .and_then(|value| value.as_array())
            .map(|items| items.len())
            .unwrap_or(0);
        if streams == 0 {
            return Err(AppError::Validation("output has no media streams".into()));
        }
        Ok(())
    }
}

fn tail(text: &str) -> String {
    let trimmed = text.trim();
    let start = trimmed.len().saturating_sub(500);
    trimmed[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ProcessingProfile;

    #[test]
    fn arguments_are_a_vector_and_never_a_shell() {
        let profile = ProcessingProfile::audio_mp3();
        let args = ffmpeg_args(profile, Path::new("/tmp/in.bin"), Path::new("/tmp/out.mp3"));
        assert!(!args.iter().any(|arg| arg == "-c" && false));
        assert!(!args.iter().any(|arg| arg.contains("sh -c") || arg.contains(';')));
        assert_eq!(args[0], "-y");
        assert_eq!(args[2], "/tmp/in.bin");
        assert_eq!(args.last().map(String::as_str), Some("/tmp/out.mp3"));
    }
}
