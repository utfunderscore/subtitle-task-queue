use whisper_rs::{FullParams, SamplingStrategy, WhisperContext};

pub trait SubtitleInference {
    fn execute(&self, sample: Vec<i16>) -> Vec<Segment>;
}

pub struct Segment {
    text: String,
    start_timestamp: i64,
    end_timestamp: i64,
}

pub struct WhisperModel<'a> {
    whisper_context: WhisperContext,
    params: FullParams<'a, 'a>,
}

impl WhisperModel<'_> {
    pub fn new(whisper_context: WhisperContext) -> Self {
        let mut params = FullParams::new(SamplingStrategy::BeamSearch {
            // whisper.cpp defaults to a beam size of 5, a reasonable default
            beam_size: 5,
            // this parameter is currently unused but defaults to -1.0
            patience: -1.0,
        });

        params.set_language(Some("en"));
        params.set_print_special(false);
        params.set_print_progress(false);
        params.set_print_realtime(false);
        params.set_print_timestamps(false);

        Self {
            whisper_context,
            params,
        }
    }
}

impl<'b> SubtitleInference for WhisperModel<'b> {
    fn execute(&self, sample: Vec<i16>) -> Vec<Segment> {
        let mut state = self
            .whisper_context
            .create_state()
            .expect("failed to create state");

        let mut inter_samples = vec![Default::default(); sample.len()];

        whisper_rs::convert_integer_to_float_audio(&sample, &mut inter_samples)
            .expect("failed to convert audio data");
        let samples = whisper_rs::convert_stereo_to_mono_audio(&inter_samples)
            .expect("failed to convert audio data");

        state
            .full(self.params.clone(), &samples[..])
            .expect("failed to run model");

        state.as_iter().map(|segment| Segment {
            text: segment.to_string(),
            start_timestamp: segment.start_timestamp(),
            end_timestamp: segment.end_timestamp(),
        }).collect()
    }
}
