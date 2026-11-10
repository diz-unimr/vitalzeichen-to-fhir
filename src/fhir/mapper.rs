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
use log::warn;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::iter;

pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
}

enum ObservationSegment<'a> {
    Single(&'a Segment<'a>),
    Multiple(BTreeMap<&'a str, &'a Segment<'a>>),
}

impl<'a> ObservationSegment<'a> {
    fn code(&self) -> Result<String, MappingError> {
        match self {
            &ObservationSegment::Single(s) => field_value(s, 3)
                .ok_or(anyhow!("OBX code field empty"))
                .map(|c| c.to_string())
                .map_err(|e| e.into()),
            ObservationSegment::Multiple(m) => Ok(m
                .values()
                .map(|s| field_value(s, 3).ok_or(anyhow!("OBX code field empty")))
                .collect::<Result<Vec<_>, _>>()?
                .join("+")),
        }
    }

    fn effective(&self) -> Result<ObservationEffective, MappingError> {
        match self {
            &ObservationSegment::Single(s) => Ok(observation_effective(s)?),
            ObservationSegment::Multiple(m) => {
                Ok(observation_effective(m.values().next().ok_or(anyhow!(
                    "Failed to determine OBX 'effective' value. Empty multi segments."
                ))?)?)
            }
        }
    }

    fn status(&self) -> Result<ObservationStatus, MappingError> {
        match self {
            &ObservationSegment::Single(s) => Ok(observation_status(s)),
            ObservationSegment::Multiple(m) => Ok(observation_status(m.values().next().ok_or(
                anyhow!("Failed to determine OBX 'status' value. Empty multi segments."),
            )?)),
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
                    .source(self.config.meta_source.clone())
                    .build()?,
            )
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn map_resources(&self, msg: &Message) -> Result<Vec<Option<BundleEntry>>, MappingError> {
        // todo: should be OBR.3 (order number) instead of MSH.10 (message key)
        // todo: breaking change: this will change Observation ids!
        let msh_id = message_key(msg)?;
        let pid = query(msg, "PID.3").ok_or(anyhow!("Missing patient id (pid) in PID.3"))?;
        let visit_id = query(msg, "PV1.19").ok_or(anyhow!("Missing encounter id (visit_id) in PV1.19"))?;

        // partition segments by blood pressure values (bp) and other, single observations
        let (bp, other): (BTreeMap<_, _>, Vec<_>) = msg
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
            .filter_map(|s| self.map_observation(s, msh_id, pid, visit_id).ok().flatten())
            .filter_map(|o| bundle_entry(o).ok())
            .collect::<Vec<_>>();

        if obs.is_empty() {
            warn!(
                "No observations could be mapped for message key={}",
                message_key(msg).unwrap_or_default()
            );
            return Ok(vec![]);
        }

        let result_refs = obs
            .iter()
            .filter_map(|e| e.request.as_ref().map(|r| r.url.clone()));
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

    fn encounter_ref(&self, visit_id: &str) -> Result<Reference, BuilderError> {
        Reference::builder()
            .reference(format!(
                "Encounter?identifier={}|{visit_id}",
                self.config.encounter_system
            ))
            .build()
    }

    fn map_respiratory_rate(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec!["https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/atemfrequenz|2025.0.4".to_string()])?)
            .code(respiratory_rate_coding(obx_code(s)?)?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("breaths per minute".to_string())
                .code("/min".to_string()).build()?))
        )
    }

    fn map_oxygen_saturation(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec!["https://gematik.de/fhir/isik/StructureDefinition/sd-mii-icu-o2saettigung-im-arteriellen-blut-durch-pulsoxymetrie".to_string()])?)
            .code(oxygen_saturation_coding(obx_code(s)?)?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("percent".to_string())
                .code("%".to_string()).build()?))
        )
    }

    fn map_heart_rate(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec!["https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/herzfrequenz|2025.0.4".to_string()])?)
            .code(heart_rate_coding(obx_code(s)?)?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("beats per minute".to_string())
                .code("/min".to_string()).build()?))
        )
    }

    fn map_weight(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec!["https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/koerpergewicht|2025.0.4".to_string()])?)
            .code(weight_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("kilogram".to_string())
                .code("kg".to_string()).build()?))
        )
    }

    fn map_height(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec!["https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/koerpergroesse|2025.0.4".to_string()])?)
            .code(height_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("centimeter".to_string())
                .code("cm".to_string()).build()?))
        )
    }

    fn map_temperature(&self, s: &Segment) -> Result<ObservationBuilder, MappingError> {
        Ok(Observation::builder()
            .meta(self.meta(vec![
                "https://gematik.de/fhir/isik/StructureDefinition/sd-mii-icu-koerpertemperatur-generisch".to_string(),
                "https://gematik.de/fhir/isik/StructureDefinition/sd-mii-icu-koerpertemperatur-trommelfell".to_string()])?)
            .code(temperature_coding()?)
            .value(ObservationValue::Quantity(Quantity::builder()
                .system("http://unitsofmeasure.org".to_string())
                .value(obx_value_numeric(s)?)
                .unit("degree Celsius".to_string())
                .code("Cel".to_string()).build()?))
            .body_site(CodeableConcept::builder()
                .coding(vec![
                    Some(Coding::builder()
                        .system("http://snomed.info/sct".to_string())
                        .code("42859004".to_string())
                        .display("Tympanic membrane structure (body structure)".to_string())
                        .build()?)
                ])
                .build()?)
        )
    }

    fn map_blood_pressure(
        &self,
        sys: &Segment,
        dia: &Segment,
        map: &Segment,
    ) -> Result<ObservationBuilder, MappingError> {
        let sys_code = obx_code(sys)?;
        let dia_code = obx_code(dia)?;
        let map_code = obx_code(map)?;
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
                                Some(local_coding(sys_code)?)
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
                                Some(local_coding(dia_code)?)
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
                                        .display("Mean blood pressure".to_string())
                                        .build()?,
                                ),
                                Some(local_coding(map_code)?)
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
            .meta(self.meta(vec!["https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/arterieller-blutdruck|2025.0.4".to_string()])?)
            .code(blood_pressure_coding(vec![sys_code, dia_code, map_code])?)
            .component(components)
        )
    }

    fn meta(&self, profiles: Vec<String>) -> Result<Meta, BuilderError> {
        Meta::builder()
            .source(self.config.meta_source.to_string())
            .profile(profiles.into_iter().map(Some).collect())
            .last_updated(Instant(OffsetDateTime::now_utc()))
            .build()
    }

    fn map_observation(
        &self,
        s: ObservationSegment,
        msh_id: &str,
        pid: &str,
        visit_id: &str,
    ) -> Result<Option<Observation>, MappingError> {
        match s {
            ObservationSegment::Single(s) => {
                // OBX identifier
                match obx_code(s)? {
                    // atemfrequenz (assumes RESPMODE: Pleth)
                    "RESP" => self.map_respiratory_rate(s),
                    // sauerstoffsättigung
                    "SPO2" => self.map_oxygen_saturation(s),
                    // herzfrequenz
                    "HR" => self.map_heart_rate(s),
                    // koerpergroesse
                    "HT" => self.map_height(s),
                    // koerpergewicht
                    "WT" => self.map_weight(s),
                    // koerpertemperatur-trommelfell (assumes TEMPSITE: Tympanic)
                    "TEMP" => self.map_temperature(s),
                    "RESPMODE" | "TEMPSITE" | "PAIN" | "BMI" => return Ok(None),
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
            .identifier(identifier(&s, msh_id)?)
            .category(category()?)
            .status(s.status()?)
            .subject(self.patient_ref(pid)?)
            .encounter(self.encounter_ref(visit_id)?)
            .effective(s.effective()?)
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
            .meta(
                Meta::builder()
                    .last_updated(Instant(OffsetDateTime::now_utc()))
                    .source(self.config.meta_source.clone())
                    .build()?,
            )
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system("https://fhir.diz.uni-marburg.de/sid/vital-sign-order-id".to_string())
                    .value(order_number(msg)?.to_string())
                    .build()?,
            )])
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

fn obx_code<'a>(s: &'a Segment<'_>) -> Result<&'a str, MappingError> {
    field_value(s, 3).ok_or(anyhow!("empty OBX identifier").into())
}

fn blood_pressure_coding<'a>(local_codes: impl IntoIterator<Item=&'a str>) -> Result<CodeableConcept, BuilderError> {
    let local_codings: Vec<Option<Coding>> = local_codes.into_iter()
        .map(|c| local_coding(c).map(Some))
        .collect::<Result<Vec<Option<_>>, _>>()?;

    let builder = CodeableConcept::builder()
        .coding(vec![
            Some(
                Coding::builder()
                    .system("http://snomed.info/sct".to_string())
                    .code("364090009".to_string())
                    .display("Systemic arterial pressure (observable entity)".to_string())
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("http://loinc.org".to_string())
                    .code("85354-9".to_string())
                    .display("Blood pressure panel with all children optional".to_string())
                    .build()?,
            ),
        ].into_iter().chain(local_codings).collect());
    builder.build()
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

fn identifier(
    s: &ObservationSegment,
    order_id: &str,
) -> Result<Vec<Option<Identifier>>, MappingError> {
    Ok(vec![Some(build_usual_identifier(
        vec![order_id, s.code()?.as_str()],
        "https://fhir.diz.uni-marburg.de/sid/vital-sign-observation-id".to_string(),
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

fn respiratory_rate_coding(local_code: &str) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("86290005".to_string())
                .display("Respiratory rate (observable entity)".to_string())
                .build()
                .ok(),
            Coding::builder()
                .system("http://loinc.org".to_string())
                .code("9279-1".to_string())
                .display("Respiratory rate".to_string())
                .build()
                .ok(),
            local_coding(local_code).ok()
        ])
        .build()
}

fn oxygen_saturation_coding(local_code: &str) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("442476006".to_string())
                .display("Arterial oxygen saturation".to_string())
                .build()?),
            Some(Coding::builder()
                .system("http://loinc.org".to_string())
                .code("59408-5".to_string())
                .display("Oxygen saturation in Arterial blood by Pulse oximetry".to_string())
                .build()?),
            Some(Coding::builder()
                .system("urn:iso:std:iso:11073:10101".to_string())
                .code("150324".to_string())
                .display("MDC_SAT_O2_ART".to_string())
                .build()?),
            Some(Coding::builder()
                .system("http://loinc.org".to_string())
                .code("2708-6".to_string())
                .display("Oxygen saturation in Arterial blood".to_string())
                .build()?),
            Some(local_coding(local_code)?)
        ])
        .build()
}

fn heart_rate_coding(local_code: &str) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("364075005".to_string())
                .display("Heart rate (observable entity)".to_string())
                .build()?),
            Some(Coding::builder()
                .system("http://loinc.org".to_string())
                .code("8867-4".to_string())
                .display("Heart rate".to_string())
                .build()?),
            Some(Coding::builder()
                .system("urn:iso:std:iso:11073:10101".to_string())
                .code("147842".to_string())
                .display("MDC_ECG_HEART_RATE".to_string())
                .build()?),
            Some(local_coding(local_code)?)
        ])
        .build()
}

fn weight_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(Coding::builder()
                .system("http://loinc.org".to_string())
                .code("29463-7".to_string())
                .display("Body weight".to_string())
                .build()?),
            Some(Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("27113001".to_string())
                .display("Body weight (observable entity)".to_string())
                .build()?),
        ])
        .build()
}

fn height_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(Coding::builder()
                .system("http://loinc.org".to_string())
                .code("8302-2".to_string())
                .display("Body height".to_string())
                .build()?),
            Some(Coding::builder()
                .system("http://snomed.info/sct".to_string())
                .code("1153637007".to_string())
                .display("Body height (observable entity)".to_string())
                .build()?),
        ])
        .build()
}

fn temperature_coding() -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![
            Some(
                Coding::builder()
                    .system("http://snomed.info/sct".to_string())
                    .code("415974002".to_string())
                    .display(
                        "Core body temperature measured at tympanic membrane (observable entity)"
                            .to_string(),
                    )
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("http://loinc.org".to_string())
                    .code("8333-7".to_string())
                    .display("Tympanic membrane temperature".to_string())
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("http://loinc.org".to_string())
                    .code("8329-5".to_string())
                    .display("Body temperature - Core".to_string())
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("http://loinc.org".to_string())
                    .code("8310-5".to_string())
                    .display("Body temperature".to_string())
                    .build()?,
            ),
            Some(
                Coding::builder()
                    .system("urn:iso:std:iso:11073:10101".to_string())
                    .code("150392".to_string())
                    .build()?,
            ),
        ])
        .build()
}

fn local_coding(code: &str) -> Result<Coding, BuilderError> {
    Coding::builder()
        .system("https://fhir.diz.uni-marburg.de/CodeSystem/vital-sign-code".to_string())
        .code(code.to_string())
        .build()
}

fn field_value<'a>(segment: &'a Segment, field: usize) -> Option<&'a str> {
    segment.field(field).map(Field::raw_value)
}

pub(crate) fn message_key<'a>(msg: &'a Message<'_>) -> Result<&'a str, MappingError> {
    query(msg, "MSH.10")
        .ok_or(anyhow!(
            "Failed to parse message key from MSH.10".to_string()
        ))
        .map_err(|e| e.into())
}

fn order_number<'a>(msg: &'a Message<'_>) -> Result<&'a str, MappingError> {
    query(msg, "OBR.3")
        .ok_or(anyhow!(
            "Failed to parse order number from OBR.3".to_string()
        ))
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
    use crate::config::Fhir;
    use crate::fhir::mapper::FhirMapper;
    use hl7_parser::Message;

    #[test]
    fn map_resources_sets_meta() {
        // assert BundleEntry.fullUrl and Meta.lastUpdated are set
        let hl7 = r#"MSH|^~\&|Connex|Welch Allyn|HIS||20251102130019||ORU^R01|test-msg|P|2.5
PID|||1234567||Lustig^Peter
PV1||I|||||||||||||||||12345678
OBR|||123|S|||20251102130008||||||||||||||||||F
OBX|1|NM|SYS||117|Mmhg||N|||F|||20251102130008|100078484223|||NIBP
OBX|2|NM|DIA||70|Mmhg||N|||F|||20251102130008|100078484223|||NIBP
OBX|3|NM|MAP||85.9|Mmhg||N|||F|||20251102130008|100078484223|||NIBP
OBX|4|NM|HR||82|/min||N|||F|||20251102130008|100078484223|||NIBP
OBX|5|ST|TEMPSITE||Tympanic||||||F|||20251102130008|100078484223|||Temperature
OBX|6|NM|TEMP||36.5|Celsius||N|||F|||20251102130008|100078484223|||Temperature
"#;
        let msg = Message::parse_with_lenient_newlines(hl7, true).unwrap();

        // act
        let mapper = FhirMapper {
            config: Fhir::default(),
        };
        let actual = mapper.map_resources(&msg).unwrap();

        assert!(actual.iter().all(|b| {
            b.as_ref().is_some_and(|e| {
                e.full_url.is_some()
                    && e.resource.as_ref().is_some_and(|r| {
                    r.as_base_resource()
                        .meta()
                        .as_ref()
                        .unwrap()
                        .last_updated
                        .is_some()
                })
            })
        }));
    }

    #[test]
    fn empty_observations_is_ok() {
        let hl7 = r#"MSH|^~\&|Connex|Welch Allyn|HIS||20251102130019||ORU^R01|test-msg|P|2.5
PID|||1234567||Lustig^Peter
PV1||I|||||||||||||||||12345678
OBR|||123|S|||20251102130008||||||||||||||||||F
OBX|1|NM|PAIN||4|||N|||F|||20261028154706|100008113623|||Manual
"#;

        // act
        let mapper = FhirMapper {
            config: Fhir::default(),
        };
        let actual = mapper.map(hl7);

        assert_eq!(actual.unwrap(), None);
    }
}
