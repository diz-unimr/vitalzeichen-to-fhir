use crate::config::Fhir;
use crate::error::MappingError;
use anyhow::anyhow;
use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::{
    BundleType, DiagnosticReportStatus, HTTPVerb, IdentifierUse, ObservationStatus,
};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, DiagnosticReport, IdentifiableResource, Observation,
    ObservationBuilder, ObservationComponent, ObservationComponentValue, ObservationEffective,
    ObservationValue, Resource, ResourceType,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity, Reference};
use fhir_model::time::OffsetDateTime;
use fhir_model::{BuilderError, DateTime, Instant};
use hl7_parser::Message;
use hl7_parser::message::{Field, Segment};
use itertools::{Either, Itertools};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::iter;

pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
}

enum ObservationSegment<'a> {
    Single(&'a Segment<'a>),
    Multiple(HashMap<&'a str, &'a Segment<'a>>),
}

impl<'a> ObservationSegment<'a> {
    fn single(&self) -> Option<&'a Segment<'a>> {
        match self {
            &ObservationSegment::Single(&ref s) => Some(s),
            ObservationSegment::Multiple(m) => m.values().next().map(|v| &**v),
        }
    }
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
        let pid = query(msg, "PID.3").ok_or(anyhow!("Missing patient id (pid) in PID.3"))?;
        let msg_key = message_key(msg)?;

        // partition segments by blood pressure values (bp) and other, single observations
        let (bp, other): (HashMap<_, _>, Vec<_>) = msg
            .segments()
            .filter(|s| s.name == "OBX")
            .partition_map(|s| match field_value(s, 3) {
                Some(id @ ("SYS" | "DIA" | "MAP")) => Either::Left((id, s)),
                _ => Either::Right(s),
            });

        // wrap both in ObservationSegment and map to Observation bundle entries
        let obs = other
            .into_iter()
            .map(ObservationSegment::Single)
            .chain(iter::once(ObservationSegment::Multiple(bp)))
            .filter_map(|s| self.map_observation(s, msg_key, pid).transpose())
            .map(|o| bundle_entry(o?))
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

    fn patient_ref(&self, pid: &str) -> Result<Reference, BuilderError> {
        Reference::builder()
            .reference(format!(
                "Patient?identifier={}|{pid}",
                self.config.patient_system
            ))
            .build()
    }

    fn map_respiratory_rate(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta("https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/atemfrequenz".to_string())?)
            .code(respiratory_rate_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("breaths per minute".to_string())
                .code("/min".to_string()).build()?))
        )
    }

    fn map_oxygen_saturation(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta("https://gematik.de/fhir/isik/StructureDefinition/sd-mii-icu-o2saettigung-im-arteriellen-blut-durch-pulsoxymetrie".to_string())?)
            .code(oxygen_saturation_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("percent".to_string())
                .code("%".to_string()).build()?))
        )
    }

    fn map_heart_rate(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta("https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/herzfrequenz".to_string())?)
            .code(heart_rate_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("beats per minute".to_string())
                .code("/min".to_string()).build()?))
        )
    }

    fn map_blood_pressure(
        &self,
        sys: &Segment,
        dia: &Segment,
        map: &Segment,
    ) -> Result<ObservationBuilder, MappingError> {
        let components = vec![
            // sys
            Some(
                ObservationComponent::builder()
                    .code(
                        CodeableConcept::builder()
                            .coding(vec![
                                Some(
                                    Coding::builder()
                                        .system("http://loinc.org".to_string())
                                        .code("8480-6".to_string())
                                        .display("Systolic blood pressure".to_string())
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("http://snomed.info/sct".to_string())
                                        .code("271649006".to_string())
                                        .display(
                                            "Systolic blood pressure (observable entity)"
                                                .to_string(),
                                        )
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("urn:iso:std:iso:11073:10101".to_string())
                                        .code("150017".to_string())
                                        .display("Systolic blood pressure".to_string())
                                        .build()?,
                                ),
                            ])
                            .build()?,
                    )
                    .value(ObservationComponentValue::Quantity(
                        Quantity::builder()
                            .value(obx_value_numeric(sys)?)
                            .unit("millimeter Mercury column".to_string())
                            .system("http://unitsofmeasure.org".to_string())
                            .code("mm[Hg]".to_string())
                            .build()?,
                    ))
                    .build()?,
            ),
            // dia
            Some(
                ObservationComponent::builder()
                    .code(
                        CodeableConcept::builder()
                            .coding(vec![
                                Some(
                                    Coding::builder()
                                        .system("http://loinc.org".to_string())
                                        .code("8462-4".to_string())
                                        .display("Diastolic blood pressure".to_string())
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("http://snomed.info/sct".to_string())
                                        .code("271650006".to_string())
                                        .display(
                                            "Diastolic blood pressure (observable entity)"
                                                .to_string(),
                                        )
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("urn:iso:std:iso:11073:10101".to_string())
                                        .code("150018".to_string())
                                        .display("Diastolic blood pressure".to_string())
                                        .build()?,
                                ),
                            ])
                            .build()?,
                    )
                    .value(ObservationComponentValue::Quantity(
                        Quantity::builder()
                            .value(obx_value_numeric(dia)?)
                            .unit("millimeter Mercury column".to_string())
                            .system("http://unitsofmeasure.org".to_string())
                            .code("mm[Hg]".to_string())
                            .build()?,
                    ))
                    .build()?,
            ),
            // map
            Some(
                ObservationComponent::builder()
                    .code(
                        CodeableConcept::builder()
                            .coding(vec![
                                Some(
                                    Coding::builder()
                                        .system("http://loinc.org".to_string())
                                        .code("8478-0".to_string())
                                        .display("Mean blood pressure".to_string())
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("http://snomed.info/sct".to_string())
                                        .code("6797001".to_string())
                                        .display(
                                            "Mean blood pressure (observable entity)".to_string(),
                                        )
                                        .build()?,
                                ),
                                Some(
                                    Coding::builder()
                                        .system("urn:iso:std:iso:11073:10101".to_string())
                                        .code("150019".to_string())
                                        .display(
                                            "Mean blood pressure".to_string(),
                                        )
                                        .build()?,
                                ),
                            ])
                            .build()?,
                    )
                    .value(ObservationComponentValue::Quantity(
                        Quantity::builder()
                            .value(obx_value_numeric(map)?)
                            .unit("millimeter Mercury column".to_string())
                            .system("http://unitsofmeasure.org".to_string())
                            .code("mm[Hg]".to_string())
                            .build()?,
                    ))
                    .build()?,
            ),
        ];
        Ok(Observation::builder()
            .meta(self.meta("https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/arterieller-blutdruck".to_string())?)
            .code(blood_pressure_coding()?)
            .component(components)
        )
    }

    fn meta(&self, profile: String) -> Result<Meta, BuilderError> {
        Meta::builder()
            .source(self.config.meta_source.to_string())
            .profile(vec![profile.into()])
            .build()
    }

    fn map_observation(
        &self,
        s: ObservationSegment,
        msg_id: &str,
        pid: &str,
    ) -> Result<Option<Observation>, MappingError> {
        let ref_seg = s
            .single()
            .ok_or(anyhow!("failed to provide single observation segment"))?;
        match s {
            ObservationSegment::Single(s) => {
                // OBX identifier
                match field_value(s, 3).ok_or(anyhow!("empty OBX identifier"))? {
                    // atemfrequenz
                    "RESP" => self.map_respiratory_rate(s),
                    // sauerstoffsättigung
                    "SPO2" => self.map_oxygen_saturation(s),
                    // herzfrequenz
                    "HR" => self.map_heart_rate(s),
                    // todo
                    "HT" => return Ok(None),
                    // todo
                    "WT" => return Ok(None),
                    // todo?
                    "RESPMODE" | "TEMPSITE" | "PAIN" | "TEMP" | "BMI" => return Ok(None),
                    o => Err(anyhow!("Unknown OBX identifier: {o}").into()),
                }?
            }
            ObservationSegment::Multiple(ref m) => {
                match (m.get("SYS"), m.get("DIA"), m.get("MAP")) {
                    // arterieller blutdruck
                    (Some(sys), Some(dia), Some(map)) => self.map_blood_pressure(sys, dia, map),
                    _ => return Ok(None),
                }?
            }
        }
            // common properties
            .identifier(identifier(ref_seg, msg_id)?)
            .category(category()?)
            .status(observation_status(ref_seg))
            .subject(self.patient_ref(pid)?)
            .effective(observation_effective(ref_seg)?)
            .build()
            .map(Some)
            .map_err(|e| e.into())
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

fn blood_pressure_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(
                Coding::builder()
                    .system("http://snomed.info/sct".to_string())
                    .code("75367002".to_string())
                    .display("Blood pressure (observable entity)".to_string())
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("http://loinc.org".to_string())
                    .code("85354-9".to_string())
                    .build()?,
            ),
        ])
        .build()
}

fn observation_effective(segment: &Segment) -> Result<ObservationEffective, MappingError> {
    let dt = parse_datetime(
        field_value(segment, 14).ok_or(anyhow!("Effective datetime missing in OBX.14"))?,
    )?;
    Ok(ObservationEffective::DateTime(dt))
}

fn category() -> Result<Vec<Option<CodeableConcept>>, BuilderError> {
    Ok(vec![Some(
        CodeableConcept::builder()
            .coding(vec![Some(
                Coding::builder()
                    .system(
                        "http://terminology.hl7.org/CodeSystem/observation-category".to_string(),
                    )
                    .code("vital-signs".to_string())
                    .build()?,
            )])
            .build()?,
    )])
}

fn identifier(segment: &Segment, msg_id: &str) -> Result<Vec<Option<Identifier>>, MappingError> {
    // identifier
    let code = field_value(segment, 3).ok_or(anyhow!("OBX code field empty"))?;
    let segment_number = field_value(segment, 1).ok_or(anyhow!("OBX segment number missing"))?;

    Ok(vec![Some(build_usual_identifier(
        vec![msg_id, segment_number, code],
        "https://fhir.diz.uni-marburg.de/sid/vital-sign-id".to_string(),
    )?)])
}

fn observation_status(segment: &Segment) -> ObservationStatus {
    // OBX.11
    match field_value(segment, 11) {
        Some("F") => ObservationStatus::Final,
        Some("C") => ObservationStatus::Preliminary,
        _ => ObservationStatus::Unknown,
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

fn respiratory_rate_coding() -> Result<CodeableConcept, BuilderError> {
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
}

fn oxygen_saturation_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("442476006".to_string())
                .display("Arterial oxygen saturation".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("http://loinc.org".to_string())
                .code("59408-5".to_string())
                .display(
                    "Sauerstoffsättigung in arteriellem Blut mittels Pulsoximetrie".to_string(),
                )
                .build()
                .ok(),
            Coding::builder()
                .system("urn:iso:std:iso:11073:10101".to_string())
                .code("150324".to_string())
                .display("MDC_SAT_O2_ART".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("http://loinc.org".to_string())
                .code("2708-6".to_string())
                .display("Oxygen saturation in Arterial blood".to_string())
                .build()
                .ok(),
        ])
        .build()
        .map_err(|e| e.into())
}

fn heart_rate_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("364075005".to_string())
                .display("Heart rate (observable entity)".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("http://loinc.org".to_string())
                .code("8867-4".to_string())
                .display("Heart Rate".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("urn:iso:std:iso:11073:10101".to_string())
                .code("147842".to_string())
                .display("Heart Rate".to_string())
                .build()
                .ok(),
        ])
        .build()
        .map_err(|e| e.into())
}

fn field_value<'a>(segment: &'a Segment, field: usize) -> Option<&'a str> {
    segment.field(field).map(Field::raw_value)
}

pub(crate) fn message_key<'a>(msg: &'a Message<'_>) -> Result<&'a str, MappingError> {
    query(msg, "MSH.10")
        .ok_or(anyhow!("Failed to parse message key".to_string()))
        .map_err(|e| e.into())
}

pub(crate) fn obx_value_numeric(s: &Segment) -> Result<f64, MappingError> {
    // OBX.2
    match field_value(s, 2) {
        Some("NM") => {
            // OBX.5
            field_value(s, 5)
                .ok_or(anyhow!("Missing OBX.5 value"))?
                .parse::<f64>()
                .map_err(|e| e.into())
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

pub(crate) fn parse_datetime(input: &str) -> Result<DateTime, MappingError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M%S")
        .map_err(|e| anyhow!("Failed to parse DateTime value from '{input}': {e}"))?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}

#[cfg(test)]
mod tests {

    // todo
    // #[test]
    // fn map_ressources_collects_errors(){}

    // #[todo]
    // fn test_parse_b
}
