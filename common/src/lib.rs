pub struct Segment {
    pub text: String,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

impl Segment {
    pub fn new(text: String, start_timestamp: i64, end_timestamp: i64) -> Self {
        Self { text, start_timestamp, end_timestamp }
    }
}
