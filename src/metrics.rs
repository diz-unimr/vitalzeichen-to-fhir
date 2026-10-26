use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::sync::OnceLock;

static PROCESS_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
static PROCESS_LATENCY: OnceLock<Histogram<u64>> = OnceLock::new();
static ERRORS: OnceLock<Counter<u64>> = OnceLock::new();

pub(crate) fn process_count() -> &'static Counter<u64> {
    PROCESS_COUNTER.get_or_init(|| {
        global::meter("processor")
            .u64_counter("records_processed_total")
            .with_description("The number of records processed")
            .build()
    })
}

pub(crate) fn process_latency() -> &'static Histogram<u64> {
    PROCESS_LATENCY.get_or_init(|| {
        global::meter("processor")
            .u64_histogram("process_duration_millis")
            .with_description("The time to fully process a record")
            // Setting boundaries is optional. By default, the boundaries are set to
            // [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0]
            //
            // .with_boundaries(vec![...])
            .build()
    })
}

pub(crate) fn errors() -> &'static Counter<u64> {
    ERRORS.get_or_init(|| {
        global::meter("processor")
            .u64_counter("errors_total")
            .with_description("The total number of errors")
            .build()
    })
}

pub(crate) fn init_meter_provider(endpoint: &str) -> anyhow::Result<SdkMeterProvider> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().with_service_name("vitalzeichen-to-fhir").build())
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(provider.clone());
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use crate::metrics::{init_meter_provider, process_count, process_latency};
    use mock_collector::{MockServer, Protocol};
    use opentelemetry::KeyValue;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_record_counter() {
        let server = MockServer::builder()
            .protocol(Protocol::Grpc)
            .start()
            .await
            .expect("Failed to start server");

        println!("Server started on {}", server.addr());

        // add metric
        let addr = format!("http://{}", server.addr());
        let provider = init_meter_provider(&addr).unwrap();

        // processed counter
        process_count().add(1, &[KeyValue::new("status", "ok")]);

        // process latency
        process_latency().record(400, &[]);

        provider.shutdown().unwrap();

        println!("Metrics sent successfully!\n");
        println!("Performing assertions...");

        server
            .with_collector(|collector| {
                // processed records metric exists
                collector
                    .expect_metric_with_name("records_processed_total")
                    .with_resource_attributes([("service.name", "vitalzeichen-to-fhir")])
                    .with_attribute("status", "ok")
                    .with_value_eq(1)
                    .assert_exists();

                // latency histogram exists
                collector
                    .expect_histogram("process_duration_millis")
                    .with_sum_eq(400)
                    .assert_exists();

                assert_eq!(collector.metric_count(), 2);
            })
            .await;

        println!("\nAll assertions passed!");

        server.shutdown().await.unwrap();
        println!("Server shut down successfully");
    }
}
