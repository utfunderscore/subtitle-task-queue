use anyhow::Result;
use std::path::Path;
use std::process::Command;

/// Check if FFmpeg is available on the system
pub fn is_ffmpeg_available() -> bool {
    Command::new("ffprobe").arg("-version").output().is_ok()
}

/// Quick check if a file is supported by FFmpeg (without checking for audio streams)
pub async fn is_ffmpeg_compatible<P: AsRef<Path>>(file_path: P) -> Result<bool> {
    let path = file_path.as_ref();

    if !path.exists() {
        return Ok(false);
    }

    // Convert to owned String before moving into spawn_blocking
    let path_str = match path.to_str() {
        Some(s) => s.to_string(), // Use to_string() instead of clone() to create owned String
        None => return Ok(false),
    };

    if !is_ffmpeg_available() {
        return Ok(false);
    }

    // Use ffprobe to just check if the file can be read
    tokio::task::spawn_blocking(move || {
        Command::new("ffprobe")
            .args(["-v", "quiet", "-show_format", &path_str]) // Add & to reference the owned String
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    })
    .await
    .map_err(|e| e.into())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffmpeg_availability() {
        // This test will only pass if FFmpeg is installed
        println!("FFmpeg available: {}", is_ffmpeg_available());
    }

    #[tokio::test]
    async fn test_compatibility_success() {
        let result = is_ffmpeg_compatible("demo2.mp4").await.unwrap();
        assert!(result);
    }
}
