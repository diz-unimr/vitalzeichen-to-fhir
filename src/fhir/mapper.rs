use crate::config::Fhir;
use crate::error::MappingError;
use crate::error::MappingError::FatalError;
use anyhow::anyhow;
use fhir_model::r4b::codes::{
    BundleType, DiagnosticReportStatus, HTTPVerb, IdentifierUse, ObservationStatus,
};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, DiagnosticReport, IdentifiableResource, Observation,
    ObservationValue, Resource, ResourceType,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity, Reference};
use fhir_model::time::OffsetDateTime;
use fhir_model::{BuilderError, Instant};
use hl7_parser::Message;
use hl7_parser::message::{Field, Segment};
use itertools::Itertools;
use sha2::{Digest, Sha256};

pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
}

impl FhirMapper {
    pub(crate) fn new(config: Fhir) -> Result<Self, anyhow::Error> {
        Ok(FhirMapper { config })
    }

    pub(crate) fn map(&self, msg: &str) -> Result<Option<String>, MappingError> {
        // deserialize
        let v2_msg = Message::parse_with_lenient_newlines(msg, true)?;

        // map hl7 message
        let resources = self.map_resources(&v2_msg)?;

        if resources.is_empty() {
            return Ok(None);
        }

        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(resources)
            .identifier(
                Identifier::builder()
                    .value(message_key(&v2_msg)?.to_string())
                    .system("https://fhir.diz.uni-marburg.de/sid/bundle-id".to_string())
                    .build()?,
            )
            .meta(
                Meta::builder()
                    .last_updated(Instant(OffsetDateTime::now_utc()))
                    // todo source
                    // .source()
                    .build()?,
            )
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn map_resources(&self, msg: &Message) -> Result<Vec<Option<BundleEntry>>, MappingError> {
        let obs = msg
            .segments()
            .filter(|s| s.name == "OBX")
            .flat_map(|s| self.map_observation(s, message_key(msg)?))
            .map(|o| bundle_entry(o))
            .collect::<Result<Vec<_>, _>>()?;

        if obs.is_empty() {
            return Err(anyhow!(
                "No observations could be mapped for message key={}",
                message_key(msg).unwrap_or_default()
            )
                .into());
        }

        let result_refs = obs
            .iter()
            .filter_map(|e| e.request.as_ref().and_then(|r| Some(r.url.clone())));
        let report = self.map_report(msg, result_refs)?;

        Ok(bundle_entry(report)
            .into_iter()
            .chain(obs)
            .map(Some)
            .collect())
    }

    fn map_respiratory_rate(&self, s: &Segment, msg_id: &str) -> Result<Observation, MappingError> {
        let mut obs = Observation::builder()
            .meta(self.meta("https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/atemfrequenz".to_string())?)
            .category(vec![
                Some(CodeableConcept::builder()
                         .coding(vec![
                             Some(Coding::builder()
                                 .system("http://terminology.hl7.org/CodeSystem/observation-category".to_string())
                                 .code("vital-signs".to_string())
                                 .build()?)]).build()?,
                )])
            .code(respiratory_rate_coding())
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("breaths per minute".to_string())
                .code("/min".to_string()).build()?))
            // OBX.11
            .status(match s.field(11).map(Field::raw_value) {
                Some("F") => ObservationStatus::Final,
                Some("C") => ObservationStatus::Preliminary,
                _ => ObservationStatus::Unknown
            })
            .build()?;

        // identifier
        let code = field_value(s, 3).ok_or(anyhow!("OBX code field empty"))?;
        let segment_number = field_value(s, 1).ok_or(anyhow!("OBX segment number missing"))?;

        obs.identifier = vec![Some(build_usual_identifier(
            vec![msg_id, segment_number, code],
            "https://fhir.diz.uni-marburg.de/sid/vital-sign-id".to_string(),
        )?)];

        Ok(obs)
    }

    fn meta(&self, profile: String) -> Result<Meta, BuilderError> {
        Meta::builder()
            .source(self.config.meta_source.to_string())
            .profile(vec![profile.into()])
            .build()
    }

    fn map_observation(&self, s: &Segment, msg_id: &str) -> Result<Observation, MappingError> {
        // OBX identifier
        match field_value(s, 3).ok_or(anyhow!("empty OBX identifier"))? {
            // "RESPMODE"
            // atemfrequenz
            "RESP" => self.map_respiratory_rate(s, msg_id),
            o => Err(anyhow!("Unknown OBX identifier: {o}").into()),
        }
    }

    fn map_report(
        &self,
        msg: &Message,
        result_refs: impl Iterator<Item=String>,
    ) -> Result<DiagnosticReport, MappingError> {
        DiagnosticReport::builder()
            .code(
                CodeableConcept::builder()
                    .coding(vec![Some(
                        Coding::builder()
                            .system("http://loinc.org".to_string())
                            .code("29274-8".to_string())
                            .display("Vital signs measurements".to_string())
                            .build()?,
                    )])
                    .build()?,
            )
            .status(match query(msg, "OBR.25") {
                Some("F") => DiagnosticReportStatus::Final,
                Some("R") => DiagnosticReportStatus::Preliminary,
                _ => DiagnosticReportStatus::Unknown,
            })
            .result(
                result_refs
                    .map(|r| Reference::builder().reference(r).build().ok())
                    .collect_vec(),
            )
            .build()
            .map_err(|e| e.into())
    }
}

pub(crate) fn build_usual_identifier(
    value_components: Vec<&str>,
    system: String,
) -> Result<Identifier, BuilderError> {
    let identifier_value = value_components.join("_");
    let hash = Sha256::digest(identifier_value.as_bytes());

    Identifier::builder()
        .r#use(IdentifierUse::Usual)
        .system(system)
        .value(base16ct::lower::encode_string(&hash))
        .build()
}

fn respiratory_rate_coding() -> CodeableConcept {
    CodeableConcept::builder()
        .coding(vec![
            Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("86290005".to_string())
                .version(
                    "http://snomed.info/sct/900000000000207008/version/20241101"
                        .to_string()
                        .into(),
                )
                .display("Respiratory rate (observable entity)".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("http://loinc.org".to_string())
                .code("9279-1".to_string())
                .display("Respiratory Rate".to_string())
                .build()
                .ok(),
        ])
        .build()
        .unwrap()
}

fn field_value<'a>(segment: &'a Segment, field: usize) -> Option<&'a str> {
    segment.field(field).map(Field::raw_value)
}

pub(crate) fn message_key<'a>(msg: &'a Message<'_>) -> Result<&'a str, MappingError> {
    query(msg, "MSH.10").ok_or(FatalError("Failed to parse message key".to_string()))
}

pub(crate) fn obx_value_numeric(s: &Segment) -> Result<f64, MappingError> {
    // OBX.2
    match field_value(s, 2) {
        Some("NM") => {
            // OBX.5
            let blubb = field_value(s, 5).ok_or(anyhow!("Missing OBX.5 value"))?;
            blubb.parse::<f64>().map_err(|e| e.into())
        }
        v => Err(anyhow!("Invalid OBX.2 value type: {v:?}. Expected numeric (NM)").into()),
    }
}

/// Query message value by location.
///
/// # Examples
/// ```
/// let value = query(msg, "PID.1");
/// ```
/// [`None`] is returned if segments are empty or missing.
pub(crate) fn query<'a>(msg: &'a Message<'_>, location: &str) -> Option<&'a str> {
    msg.query(location)
        .map(|l| l.raw_value())
        .filter(|s| !s.is_empty())
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
) -> Result<BundleEntry, MappingError>
where
    Resource: From<T>,
{
    // resource
    let r = Resource::from(resource.clone());

    // identifier
    let identifier = resource
        .identifier()
        .iter()
        .flatten()
        .find(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .ok_or(anyhow!("missing identifier with use: 'usual'"))?;

    // resource type
    let resource_type = r.resource_type();

    BundleEntry::builder()
        .resource(r)
        .request(bundle_entry_request(resource_type, identifier)?)
        .full_url(format!("urn:uuid:{}", uuid::Uuid::new_v4()))
        .build()
        .map_err(|e| e.into())
}

fn bundle_entry_request(
    resource_type: ResourceType,
    identifier: &Identifier,
) -> Result<BundleEntryRequest, MappingError> {
    Ok(BundleEntryRequest::builder()
        .method(HTTPVerb::Put)
        .url(format!(
            "{resource_type}?identifier={}|{}",
            identifier
                .system
                .as_deref()
                .ok_or(anyhow!("identifier.system missing"))?,
            identifier
                .value
                .as_deref()
                .ok_or(anyhow!("identifier.value missing"))?
        ))
        .build()?)
}

#[cfg(test)]
mod tests {}
