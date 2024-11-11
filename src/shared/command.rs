use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::application::AppError;

pub struct CommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

const CAPTURE_LIMIT: usize = 64 * 1024;

/// Run a program with an argument vector. This never invokes a shell.
pub async fn run(
    program: &Path,
    args: &[String],
    timeout: Duration,
    stdout_progress: Option<tokio::sync::mpsc::Sender<String>>,
) -> Result<CommandOutput, AppError> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| {
        AppError::Dependency(format!("failed to start {}: {error}", program.display()))
    })?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(capture(stdout, stdout_progress));
    let stderr_task = tokio::spawn(capture(stderr, None));
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result.map_err(|error| AppError::Dependency(error.to_string()))?,
        Err(_) => {
            let _ = child.start_kill();
            return Err(AppError::Dependency(format!("{} timed out", program.display())));
        }
    };
    let stdout = stdout_task.await.map_err(|error| AppError::Dependency(error.to_string()))?;
    let stderr = stderr_task.await.map_err(|error| AppError::Dependency(error.to_string()))?;
    Ok(CommandOutput { status: status.code().unwrap_or(-1), stdout, stderr })
}

async fn capture<R>(
    reader: Option<R>,
    progress: Option<tokio::sync::mpsc::Sender<String>>,
) -> String
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return String::new();
    };
    let mut collected = Vec::new();
    let mut buf = vec![0_u8; 4096];
    let mut line = Vec::new();
    loop {
        let read = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if collected.len() < CAPTURE_LIMIT {
            let room = CAPTURE_LIMIT - collected.len();
            collected.extend_from_slice(&buf[..read.min(room)]);
        }
        if let Some(tx) = &progress {
            for byte in &buf[..read] {
                if *byte == b'\n' {
                    if let Ok(text) = String::from_utf8(std::mem::take(&mut line)) {
                        let _ = tx.try_send(text);
                    }
                } else if line.len() < 1024 {
                    line.push(*byte);
                }
            }
        }
    }
    String::from_utf8_lossy(&collected).into_owned()
}

pub fn parse_download_percent(line: &str) -> Option<u8> {
    let marker = line.find('[')?;
    let rest = &line[marker..];
    if !rest.contains('%') {
        return None;
    }
    let token = rest.split_whitespace().find(|part| part.ends_with('%'))?;
    let number = token.trim_end_matches('%');
    let value: f64 = number.parse().ok()?;
    if (0.0..=100.0).contains(&value) { Some(value as u8) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ytdlp_progress_lines() {
        assert_eq!(parse_download_percent("[download]  42.5% of 10.00MiB"), Some(42));
        assert_eq!(parse_download_percent("nothing here"), None);
    }
}
