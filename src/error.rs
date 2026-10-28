use fhir_model::time::error::ComponentRange;
use fhir_model::{BuilderError, DateFormatError};
use hl7_parser::parser::ParseError;
use rdkafka::error::KafkaError;
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
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    TimeParseError(#[from] chrono::format::ParseError),
    #[error(transparent)]
    TimeRangeError(#[from] ComponentRange),
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    ParseFloatError(#[from] ParseFloatError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
