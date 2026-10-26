use anyhow::anyhow;
use config::{Config, Environment, File};
use serde::Deserialize;
use validator::Validate;

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct App {
    pub(crate) log_level: String,
    pub(crate) telemetry_endpoint: String,
}

#[derive(Default, Deserialize, Clone, Debug, Validate)]
pub(crate) struct Kafka {
    pub(crate) brokers: String,
    pub(crate) security_protocol: String,
    pub(crate) ssl: Option<Ssl>,
    pub(crate) consumer_group: String,
    pub(crate) input_topic: String,
    pub(crate) output_topic: String,
    pub(crate) offset_reset: String,
    #[validate(range(min = 1, max = 20))]
    pub(crate) num_partitions: i32,
}

#[derive(Default, Deserialize, Clone)]
pub(crate) struct Fhir {
    pub(crate) meta_source: String,
    pub(crate) profile: Vec<Profile>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct Profile {
    pub(crate) name: String,
    pub(crate) canonical: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Ssl {
    pub(crate) ca_location: Option<String>,
    pub(crate) certificate_location: Option<String>,
    pub(crate) key_location: Option<String>,
    pub(crate) key_password: Option<String>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct AppConfig {
    pub(crate) app: App,
    pub(crate) kafka: Kafka,
    pub(crate) fhir: Fhir,
}

impl AppConfig {
    pub(crate) fn new() -> anyhow::Result<Self> {
        Self::with_env(Environment::default().separator("."))
    }
    fn with_env(env: Environment) -> anyhow::Result<Self> {
        Config::builder()
            // default config from file
            .add_source(File::with_name("app.yaml"))
            // override values from environment variables
            .add_source(env)
            .build()?
            // .map_err(|e| anyhow!(e))
            .try_deserialize::<Self>()
            // validate
            .map(|c| match c.kafka.validate() {
                Ok(()) => Ok(c),
                Err(e) => Err(anyhow!(e)),
            })?
    }
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;
    use config::Environment;
    use std::collections::HashMap;

    #[test]
    fn invalid_config_fails() {
        // override validated property with invalid data
        let source = Environment::default().source(Some({
            let mut env = HashMap::new();
            env.insert("kafka.num_partitions".into(), "0".into());
            env
        }));

        let c = AppConfig::with_env(source);

        assert!(c.is_err());
    }
}
