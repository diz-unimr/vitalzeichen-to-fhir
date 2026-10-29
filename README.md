# vitalzeichen-to-fhir

[![MegaLinter](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/mega-linter.yml/badge.svg)](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/mega-linter.yml)
[![build](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/build.yaml/badge.svg)](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/build.yaml)
[![docker](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/release.yaml/badge.svg)](https://github.com/diz-unimr/vitalzeichen-to-fhir/actions/workflows/release.yaml)
[![codecov](https://codecov.io/gh/diz-unimr/vitalzeichen-to-fhir/graph/badge.svg?token=urFEEfhEJB)](https://codecov.io/gh/diz-unimr/vitalzeichen-to-fhir)

> Vitalzeichen to FHIR 🔥 Kafka processor

## Offset handling

The consumer is configured to auto-commit offsets, in order to improve performance. Successfully processed (and
produced) records are committed manually to the offset store (`enable.auto.offset.store`) to be eligible for
auto-commiting.

## Mapping

Maps vital signs from HL7v2 messages to FHIR 🔥. Input data is produced by Welch Allyn Connex Spot monitors.

## Configuration properties

Application properties are read from a properties file ([app.yaml](./app.yaml)) with default values.

| Name                             | Default                                                           | Description                                   |
|----------------------------------|-------------------------------------------------------------------|-----------------------------------------------|
| `app.log_level`                  | info                                                              | Log level (error,warn,info,debug,trace)       |
| `app.telemetry_endpoint`         | <http://localhost:4317>                                           |                                               |
| `kafka.brokers`                  | localhost:9092                                                    | Kafka brokers                                 |
| `kafka.security_protocol`        | plaintext                                                         | Kafka communication protocol                  |
| `kafka.ssl.ca_location`          | /app/cert/kafka_ca.pem                                            | Kafka CA certificate location                 |
| `kafka.ssl.certificate_location` | /app/cert/app_cert.pem                                            | Client certificate location                   |
| `kafka.ssl.key_location`         | /app/cert/app_key.pem                                             | Client key location                           |
| `kafka.ssl.key_password`         |                                                                   | Client key password                           |
| `kafka.consumer_group`           | vitalzeichen-to-fhir                                              | Consumer group name                           |
| `kafka.input_topic`              | vitalzeichen-hl7                                                  | Kafka topic to consume                        |
| `kafka.output_topic`             | vitalzeichen-fhir                                                 | Kafka output topic                            |
| `kafka.offset_reset`             | earliest                                                          | Kafka consumer reset (`earliest` or `latest`) |
| `fhir.meta_source`               | <https://fhir.diz.uni-marburg.de/data-source/connex-spot-monitor> | Value of `resource.meta.source`               |
| `fhir.profile`                   | `...`                                                             | `Patient` FHIR profile                        |

### Environment variables

Override configuration properties by providing environment variables with their respective property names.

## Deployment

> [!WARNING]
> TODO Example docker compose

## License

[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)
