use fhir_model::BuilderError;
use hl7_parser::parser::ParseError;
use rdkafka::error::KafkaError;
use std::fmt::{Display, Formatter};
use std::num::ParseFloatError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ProcessingError {
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Mapping(#[from] MappingError),
}
#[derive(Debug, Error)]
pub(crate) enum MappingError {
    FatalError(String),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    ParseFloatError(#[from] ParseFloatError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Display for MappingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to map HL7 message")
    }
}
