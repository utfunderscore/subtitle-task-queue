use std::fmt;
use std::str::FromStr;
use crate::TaskStage::Unknown;

pub struct Segment {
    pub text: String,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

impl Segment {
    pub fn new(text: String, start_timestamp: i64, end_timestamp: i64) -> Self {
        Self {
            text,
            start_timestamp,
            end_timestamp,
        }
    }
}

#[derive(Default)]
pub enum TaskStage {
    #[default]
    Unknown,
    UploadingFile,
    TransferingS3,
    FindingWorker,
    Processing,
    Failed,
    Success,
}

// Implement Default so TaskStage::Unknown is the default

// Display implementation for string conversions
impl fmt::Display for TaskStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TaskStage::Unknown => "Unknown",
            TaskStage::UploadingFile => "UploadingFile",
            TaskStage::TransferingS3 => "TransferingS3",
            TaskStage::FindingWorker => "FindingWorker",
            TaskStage::Processing => "Processing",
            TaskStage::Failed => "Failed",
            TaskStage::Success => "Success",
        };
        write!(f, "{}", s)
    }
}

// FromStr implementation: Unrecognized strings map to Unknown
impl FromStr for TaskStage {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        Ok(match s {
            "Unknown" => TaskStage::Unknown,
            "UploadingFile" => TaskStage::UploadingFile,
            "TransferingS3" => TaskStage::TransferingS3,
            "FindingWorker" => TaskStage::FindingWorker,
            "Processing" => TaskStage::Processing,
            "Failed" => TaskStage::Failed,
            "Success" => TaskStage::Success,
            _ => Unknown  // Default to Unknown
        })
    }
}