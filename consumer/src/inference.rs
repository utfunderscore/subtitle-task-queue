use anyhow::{Context, Result};
use common::Segment;
use whisper_rs::{FullParams, SamplingStrategy, WhisperContext};

pub trait SubtitleInference {
    fn execute(&self, sample: &Vec<i16>) -> Result<Vec<Segment>>;
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
    fn execute(&self, sample: &Vec<i16>) -> Result<Vec<Segment>> {
        let mut state = self
            .whisper_context
            .create_state()
            .context("Failed to create Whisper state")?;

        let mut samples = vec![Default::default(); sample.len()];

        whisper_rs::convert_integer_to_float_audio(sample, &mut samples)
            .context("Failed to convert integer audio samples to float")?;

        // Note: Audio is already converted to mono in convert_to_wav(),
        // so no stereo-to-mono conversion is needed here

        state
            .full(self.params.clone(), &samples[..])
            .context("Failed to run Whisper inference model")?;

        let segments = state
            .as_iter()
            .map(|segment| {
                Segment::new(
                    segment.to_string(),
                    segment.start_timestamp(),
                    segment.end_timestamp(),
                )
            })
            .collect();

        Ok(segments)
    }
}
