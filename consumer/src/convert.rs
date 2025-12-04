use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

#[derive(Debug)]
pub enum ConvertError {
    IoError(std::io::Error),
    FfmpegError(String),
    NoAudioStream,
    InvalidInput,
    WavReadError(hound::Error),
}

impl From<std::io::Error> for ConvertError {
    fn from(err: std::io::Error) -> Self {
        ConvertError::IoError(err)
    }
}

impl From<hound::Error> for ConvertError {
    fn from(err: hound::Error) -> Self {
        ConvertError::WavReadError(err)
    }
}

/// Checks if a file has an audio stream using ffprobe
fn has_audio_stream<P: AsRef<Path>>(input_path: P) -> Result<bool, ConvertError> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a:0")
        .arg("-show_entries")
        .arg("stream=codec_type")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(input_path.as_ref().as_os_str())
        .output()?;

    if !output.status.success() {
        return Err(ConvertError::FfmpegError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    let result = String::from_utf8_lossy(&output.stdout);
    Ok(result.trim() == "audio")
}

/// Converts any audio file to WAV format and returns the audio samples as Vec<i16>
/// 
/// # Arguments
/// * `input_path` - Path to the input audio/video file
/// 
/// # Returns
/// * `Ok(Vec<i16>)` - Vector of 16-bit audio samples (mono, 16kHz)
/// * `Err(ConvertError)` - Error if conversion fails
/// 
/// # Notes
/// Uses a temporary directory for the intermediate WAV file, which is automatically cleaned up
pub fn convert_to_wav<P: AsRef<Path>>(input_path: P) -> Result<Vec<i16>, ConvertError> {
    let input = input_path.as_ref();
    
    // Check if input file exists
    if !input.exists() {
        return Err(ConvertError::InvalidInput);
    }

    // Check if file has audio stream
    if !has_audio_stream(input)? {
        return Err(ConvertError::NoAudioStream);
    }

    // Create temporary directory (automatically cleaned up when dropped)
    let temp_dir = TempDir::new()?;
    let output = temp_dir.path().join("output.wav");

    // Run ffmpeg conversion to WAV (16-bit PCM, mono, 16kHz for whisper compatibility)
    let status = Command::new("ffmpeg")
        .arg("-i")
        .arg(input.as_os_str())
        .arg("-vn") // No video
        .arg("-acodec")
        .arg("pcm_s16le") // 16-bit PCM
        .arg("-ac")
        .arg("1") // Mono
        .arg("-ar")
        .arg("16000") // 16kHz sample rate (optimal for whisper)
        .arg("-y") // Overwrite output file
        .arg(output.as_os_str())
        .status()?;

    if !status.success() {
        return Err(ConvertError::FfmpegError(format!(
            "ffmpeg exited with status: {}",
            status
        )));
    }

    // Read the WAV file and extract samples
    let samples: Vec<i16> = hound::WavReader::open(&output)?
        .into_samples::<i16>()
        .collect::<Result<Vec<_>, _>>()?;

    // temp_dir is automatically dropped here, cleaning up the WAV file
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_wav() {
        // This test requires a valid audio file to be present
        // Uncomment and modify with actual test file
        // let result = convert_to_wav("test_audio.mp3");
        // assert!(result.is_ok());
    }
}