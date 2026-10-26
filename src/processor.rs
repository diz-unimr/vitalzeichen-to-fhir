use crate::ClientConfig;
use crate::config::{Kafka, Ssl};
use crate::error::{MappingError, ProcessingError};
use crate::fhir::mapper::FhirMapper;
use crate::metrics::{errors, process_count, process_latency};
use futures::TryStreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use opentelemetry::KeyValue;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

pub(crate) struct Processor {
    config: Kafka,
    mapper: Arc<FhirMapper>,
    producer: Arc<FutureProducer>,
    ctx: Context,
}

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) on_commit: Option<Sender<TopicPartitionList>>,
    pub(crate) cancel: CancellationToken,
}
type ProcessingConsumer = StreamConsumer<Context>;
impl ClientContext for Context {}
impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("[Rebalance] pre {}", format_rebalance(rebalance));
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("[Rebalance] post {}", format_rebalance(rebalance));
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        info!(
            "[Offsets] committed for {}",
            format_offsets_from_parts(offsets)
        );

        if let Some(hook) = &self.on_commit {
            match result {
                Ok(_) => {
                    let sender = hook.clone();
                    let offsets = offsets.clone();
                    tokio::spawn(async move {
                        if let Err(e) = sender.send(offsets).await {
                            error!("Failed to send commit_callback result: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("Offset commit returned error: {e}");
                }
            }
        }
    }
}

fn format_rebalance(r: &Rebalance) -> String {
    match r {
        Rebalance::Assign(t) => format!("assign {}", format_topic_partitions(t)),
        Rebalance::Revoke(t) => format!("revoke {}", format_topic_partitions(t)),
        Rebalance::Error(e) => format!("error: {e}"),
    }
}

fn format_topic_partitions(topic_parts: &TopicPartitionList) -> String {
    topic_parts
        .elements()
        .iter()
        .map(|e| (e.topic(), e.partition().to_string()))
        .into_group_map()
        .iter()
        .map(|(t, p)| format!("topic: {}, partition(s): [{}]", t, p.join(" "), ))
        .collect::<String>()
}
fn format_offsets_from_parts(topic_parts: &TopicPartitionList) -> String {
    topic_parts
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(o) => Some((e.topic(), o.to_string())),
            _ => None,
        })
        .into_group_map()
        .iter()
        .map(|(t, o)| format!("topic: {}, offset(s): [{}]", t, o.join(" ")))
        .collect::<String>()
}

impl Processor {
    pub(crate) fn new(config: Kafka, mapper: Arc<FhirMapper>, ctx: Context) -> Self {
        let producer = Arc::new(create_producer(config.clone()));
        Self {
            config,
            mapper,
            producer,
            ctx,
        }
    }

    pub(crate) async fn start(self) {
        let this = Arc::new(self);

        let tasks = (1..=this.config.num_partitions)
            .map(|id| {
                let this = this.clone();
                tokio::spawn(this.run(id))
            })
            .collect::<FuturesUnordered<_>>();

        join_all(tasks).await;
    }

    async fn run(self: Arc<Self>, id: i32) {
        loop {
            // create consumer
            let instance_id = format!("{}_{id}", self.config.consumer_group);
            let consumer = self.create_consumer(&instance_id);
            let topic = &self.config.input_topic;
            match consumer.subscribe(&[topic]) {
                Ok(()) => {
                    info!(
                        "Consumer[{id}] Successfully subscribed to topic {topic} with instance id: {instance_id}"
                    );
                }
                Err(e) => {
                    error!("Consumer[{id}] Failed to subscribe to topic {topic}: {e}");
                    // exit
                    break;
                }
            }

            let consumer = Arc::new(consumer);

            select! {
                _ = self.ctx.cancel.cancelled() =>  {
                    info!("Consumer[{id}] for topic {topic} was stopped by cancellation");
                    return
                }
                stream = consumer.stream().map_err(ProcessingError::from)
                .try_for_each(|m| async {
                    let start = Instant::now();
                    let result= self.process_message(m, id, consumer.clone()).await;
                    let duration = start.elapsed().as_millis();

                    // record latency
                    process_latency().record(
                        duration as u64,
                        &[]
                    );
                    result
                }) => {
                    info!("Starting Consumer[{instance_id}] for topic {}",
                        self.config.input_topic);
                    match stream {
                            // exit
                            Err(ProcessingError::Mapping(e)) => {
                                consumer.unsubscribe();
                                error!("{e}. Exiting.");
                                // cancel all consumer instances
                                self.ctx.cancel.cancel();
                                // exit loop
                                break;
                            }
                            // continue
                            Err(ProcessingError::Kafka(e)) => {
                                consumer.unsubscribe();
                                error!("Failed to process message: {e}. Retrying..");
                            }
                            // exit
                            Ok(()) => {
                                warn!("Consumer stream for topic {id} unexpectedly ended");
                                break;
                            }
                        };

                        info!("Restarting consumer for topic {id} in 10 seconds...");
                        if self.is_cancelled(Duration::from_secs(10)).await {
                            // The token was cancelled
                            info!("Consumer[{id}] for topic {topic} was stopped by cancellation");
                            break;
                        }
                }
            }
        }
    }

    async fn process_message(
        &self,
        m: BorrowedMessage<'_>,
        id: i32,
        consumer: Arc<ProcessingConsumer>,
    ) -> Result<(), ProcessingError> {
        let topic = m.topic();

        let (key, payload) = deserialize_message(&m);

        debug!("[Received] message from {topic}, key: {key}");
        trace!(
            "Message key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            key,
            payload.as_deref().unwrap_or("[null]"),
            m.topic(),
            m.partition(),
            m.offset(),
            m.timestamp()
        );

        if let Some(headers) = m.headers() {
            for header in headers.iter() {
                trace!(
                    "Header {}:{}",
                    header.key,
                    header
                        .value
                        .map(String::from_utf8_lossy)
                        .unwrap_or_default()
                );
            }
        }

        // filter tombstone records
        if let Some(payload) = payload {
            let result = match self.mapper.map(&payload) {
                Ok(Some(r)) => r,
                Ok(None) => {
                    consumer.store_offset_from_message(&m)?;
                    return Ok(());
                }
                // handle error
                Err(e) => {
                    error!("Failed to map payload with [key={key}]: {e}");

                    return match &e {
                        MappingError::FatalError(v) => {
                            error!("Fatal error, stopping Consumer[{id}]: {v}");
                            Err(ProcessingError::Mapping(e))
                        }
                        _ => {
                            consumer.store_offset_from_message(&m)?;
                            process_count().add(1, &[KeyValue::new("status", "error")]);
                            errors().add(1, &[KeyValue::new("type", e.to_string())]);

                            Ok(())
                        }
                    };
                }
            };

            // send to output topic
            let mut record = FutureRecord::to(&self.config.output_topic)
                .key(&key)
                .payload(result.as_str());
            record.timestamp = m.timestamp().to_millis();

            let produce_future = self.producer.send(record, Timeout::Never);
            match produce_future.await {
                Ok(delivery) => {
                    debug!(
                        "[Sent] key: {key}, partition: {}, offset: {}",
                        delivery.partition, delivery.offset
                    );
                    // store offset
                    consumer.store_offset_from_message(&m)?;
                    process_count().add(1, &[KeyValue::new("status", "ok")]);
                }
                Err((e, _)) => error!("Error producing record: {:?}", e),
            }
        }

        Ok(())
    }

    async fn is_cancelled(&self, timeout: Duration) -> bool {
        select! {
            _ =  self.ctx.cancel.cancelled() => {
                true
            }
            _ = tokio::time::sleep(timeout) => {
                false
            }
        }
    }

    fn create_consumer(&self, instance_id: &str) -> ProcessingConsumer {
        let config = self.config.clone();
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", config.brokers)
            .set("security.protocol", config.security_protocol)
            .set("enable.partition.eof", "false")
            .set("group.id", config.consumer_group)
            .set("group.instance.id", instance_id)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", config.offset_reset)
            .set_log_level(RDKafkaLogLevel::Debug);

        if let Some(ssl) = config.ssl {
            if let Some(value) = ssl.ca_location {
                c.set("ssl.ca.location", value);
            }
            if let Some(value) = ssl.key_location {
                c.set("ssl.key.location", value);
            }
            if let Some(value) = ssl.certificate_location {
                c.set("ssl.certificate.location", value);
            }
            if let Some(value) = ssl.key_password {
                c.set("ssl.key.password", value);
            }
        }

        c.create_with_context(self.ctx.clone())
            .expect("Failed to create Kafka consumer")
    }
}

fn deserialize_message(m: &BorrowedMessage) -> (String, Option<String>) {
    let key = match m.key_view::<str>() {
        None => "",
        Some(Ok(k)) => k,
        Some(Err(e)) => {
            error!("Error while deserializing message key: {:?}", e);
            ""
        }
    };
    let payload = match m.payload_view::<str>() {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            None
        }
    };

    (key.to_owned(), payload.map(str::to_string).to_owned())
}

fn create_producer(config: Kafka) -> FutureProducer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("compression.type", "gzip")
        .set("message.max.bytes", "6242880")
        .set_log_level(RDKafkaLogLevel::Debug);

    set_ssl_config(c, config.ssl)
        .create()
        .expect("Failed to create Kafka producer")
}

fn set_ssl_config(mut c: ClientConfig, ssl_config: Option<Ssl>) -> ClientConfig {
    if let Some(ssl) = ssl_config {
        if let Some(value) = ssl.ca_location {
            c.set("ssl.ca.location", value);
        }
        if let Some(value) = ssl.key_location {
            c.set("ssl.key.location", value);
        }
        if let Some(value) = ssl.certificate_location {
            c.set("ssl.certificate.location", value);
        }
        if let Some(value) = ssl.key_password {
            c.set("ssl.key.password", value);
        }
    }
    c
}

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, Kafka};
    use crate::fhir::mapper::FhirMapper;
    use crate::processor::{Context, Processor, deserialize_message};
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
    use serde_json::Value;
    use serde_json_assert::assert_json_include;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_run() {
        init_logging();
        const INPUT_TOPIC: &str = "input_topic";
        const OUTPUT_TOPIC: &str = "output_topic";

        // create mock cluster
        let mock_cluster = setup_kafka(vec![("test", "test")]).await;
        mock_cluster
            .create_topic(INPUT_TOPIC, 1, 1)
            .expect("Failed to create input topic");
        mock_cluster
            .create_topic(OUTPUT_TOPIC, 1, 1)
            .expect("Failed to create output topic");

        let test_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation failed");

        let output_consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "test-consumer")
            .create()
            .expect("Consumer creation failed");
        output_consumer.subscribe(&[OUTPUT_TOPIC]).unwrap();

        // input data
        let hl7_str = read_test_resource("test.hl7");

        let _res = send_record(test_producer.clone(), INPUT_TOPIC, hl7_str.as_str())
            .await
            .unwrap();

        // setup config
        let config = AppConfig {
            kafka: Kafka {
                brokers: mock_cluster.bootstrap_servers(),
                offset_reset: String::from("earliest"),
                security_protocol: String::from("plaintext"),
                consumer_group: String::from("test"),
                input_topic: INPUT_TOPIC.to_owned(),
                output_topic: OUTPUT_TOPIC.to_owned(),
                num_partitions: 1,
                ssl: None,
            },
            app: Default::default(),
            fhir: Default::default(),
        };
        // mapper
        let mapper = Arc::new(FhirMapper {
            config: config.fhir,
        });

        // processor
        let token = CancellationToken::new();
        let p = Processor::new(
            config.kafka,
            mapper,
            Context {
                cancel: token,
                on_commit: None,
            },
        );

        // run
        tokio::spawn(async move { p.start().await });

        // get message from output topic
        let m = output_consumer.recv().await;
        let (_, payload) = deserialize_message(&m.unwrap());
        let payload = &payload.expect("failed to read output message");
        let actual: Value = serde_json::from_str(&payload).unwrap();

        let expected: Value =
            serde_json::from_str(&read_test_resource("test-bundle.json")).unwrap();

        assert_json_include!(actual:actual,expected: expected);
    }

    #[tokio::test]
    async fn cancellation_test() {
        init_logging();

        const INPUT_TOPIC: &str = "input_topic";
        const OUTPUT_TOPIC: &str = "output_topic";

        // create mock cluster
        let mock_cluster = setup_kafka(vec![("test", "test")]).await;
        mock_cluster
            .create_topic(INPUT_TOPIC, 1, 1)
            .expect("Failed to create input topic");
        mock_cluster
            .create_topic(OUTPUT_TOPIC, 1, 1)
            .expect("Failed to create output topic");

        // setup config
        let config = AppConfig {
            kafka: Kafka {
                brokers: mock_cluster.bootstrap_servers(),
                offset_reset: String::from("earliest"),
                security_protocol: String::from("plaintext"),
                consumer_group: String::from("test"),
                input_topic: INPUT_TOPIC.to_owned(),
                output_topic: OUTPUT_TOPIC.to_owned(),
                num_partitions: 1,
                ssl: None,
            },
            app: Default::default(),
            fhir: Default::default(),
        };

        // mapper
        let mapper = Arc::new(FhirMapper {
            config: config.fhir,
        });

        // cancellation token
        let token = CancellationToken::new();
        let cloned_token = token.clone();

        // processor
        let p = Processor::new(
            config.kafka,
            mapper,
            Context {
                cancel: token.clone(),
                on_commit: None,
            },
        );

        let processor = tokio::spawn(async move { p.start().await });

        assert!(!processor.is_finished());
        cloned_token.cancel();
        // processor stopped
        assert!(processor.await.is_ok());
    }

    fn init_logging() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    async fn send_record(
        producer: FutureProducer,
        topic: &str,
        payload: &str,
    ) -> OwnedDeliveryResult {
        producer
            .send_result(
                FutureRecord::to(topic)
                    .key("test")
                    .payload(payload)
                    .timestamp(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    ),
            )
            .unwrap()
            .await
            .unwrap()
    }

    async fn setup_kafka<'a>(
        records: Vec<(&str, &str)>,
    ) -> MockCluster<'a, DefaultProducerContext> {
        // create mock cluster
        let mock_cluster = MockCluster::new(3).unwrap();
        let mock_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        for record in records {
            let _ = mock_cluster.create_topic(record.0, 3, 3);

            send_record(mock_producer.clone(), record.0, record.1)
                .await
                .unwrap();
        }

        mock_cluster
    }

    pub(crate) fn read_test_resource(file_name: &str) -> String {
        let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        file_path.push("resources/test");
        file_path.push(file_name);

        fs::read_to_string(file_path.display().to_string())
            .unwrap_or_else(|_| panic!("Test resource not found: {}", file_path.display()))
    }
}
