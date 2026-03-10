# Barito Flow

Barito Flow is a core component for handling log flow within a Barito cluster. It operates in two distinct modes:

- **Producer Mode**: Receives logs from various sources and forwards them to Kafka
- **Consumer Mode**: Consumes logs from Kafka and forwards them to Elasticsearch

## Features

- **gRPC API**: High-performance gRPC endpoints for log ingestion
- **Multiple Operation Modes**: Producer, Consumer, and Consumer-GCS modes
- **Flexible Rate Limiting**: Support for local, Redis-based, and Gubernator distributed rate limiting
- **PII Redaction**: Built-in support for sensitive data redaction via market endpoint or static rules
- **High Performance**: Optimized for high-throughput log processing
- **Flexible Configuration**: Environment-based configuration for different deployment scenarios
- **Auto-discovery**: Supports Consul for service discovery
- **mTLS Support**: Secure connections to Elasticsearch with mutual TLS
- **Resilient**: Built-in retry mechanisms and error handling

## Architecture

Barito Flow uses gRPC for high-performance log ingestion. The gRPC messages and services are declared in the [barito-proto](https://github.com/bentol/barito-proto) repository.

### Producer Mode Flow

1. Receives logs via gRPC endpoint (default port :8082)
2. Applies rate limiting per application or application group
3. Automatically creates Kafka topics if they don't exist
4. Publishes logs to appropriate Kafka topics

### Consumer Mode Flow

1. Creates topic events and generates workers
2. Consumes logs from Kafka topics
3. Applies PII redaction if configured
4. Stores logs to Elasticsearch using either bulk operations or single inserts
5. Implements retry mechanisms with backoff for failed operations

### Consumer-GCS Mode Flow

1. Discovers Kafka topics matching configured regex pattern
2. Spawns consumer workers for each matched topic
3. Writes consumed logs to Google Cloud Storage

## Development Setup

### Prerequisites

For ARM-based local machines (e.g., Apple M1, Apple M2), set the Go architecture:

```sh
go env -w GOARCH=amd64
```

### Build from Source

Fetch and build the project:

```sh
git clone https://github.com/BaritoLog/barito-flow
cd barito-flow
go build
```

### Running Test Stack using Docker Compose

First, install Docker on your local machine. 

For local Kafka development (single-node Kafka + ZooKeeper):

```sh
docker-compose up -d
```

For full stack testing (Elasticsearch, Kafka, producer, and consumer):

```sh
docker-compose -f docker/docker-compose.yml up -d
```

The ports are mapped as if they are running on local machine.

### Testing

Run unit tests:

```sh
make test
```

Check for vulnerabilities:

```sh
make vuln
```

Check for dead code:

```sh
make deadcode
```

## Producer Mode

Producer mode is responsible for:

- Receiving logs by exposing gRPC endpoints
- Producing messages to Kafka cluster

After the project is built, run:

```sh
./barito-flow producer

# or
./barito-flow p

# or with go run
go run main.go producer
```

### gRPC Endpoints

The producer exposes two gRPC methods on port :8082 (configurable via `BARITO_PRODUCER_GRPC`):

#### producer.Producer/Produce

Single log entry:

```bash
grpcurl -plaintext \
  -import-path /path/to/grpc-gateway/third_party/googleapis \
  -import-path /path/to/barito-proto \
  -proto producer/producer.proto \
  -d '{
    "context": {
      "kafka_topic": "my-app",
      "kafka_partition": 1,
      "kafka_replication_factor": 1,
      "es_index_prefix": "my-app",
      "es_document_type": "_doc",
      "app_max_tps": 100
    },
    "content": {
      "message": "hello world"
    }
  }' \
  localhost:8082 producer.Producer/Produce
```

#### producer.Producer/ProduceBatch

Batch log entries:

```bash
grpcurl -plaintext \
  -import-path /path/to/grpc-gateway/third_party/googleapis \
  -import-path /path/to/barito-proto \
  -proto producer/producer.proto \
  -d '{
    "context": {
      "kafka_topic": "my-app",
      "app_max_tps": 100
    },
    "items": [
      {"content": {"message": "log 1"}},
      {"content": {"message": "log 2"}}
    ]
  }' \
  localhost:8082 producer.Producer/ProduceBatch
```

Note: The `content` field accepts arbitrary JSON structure.

### Producer Configuration

These environment variables can be modified to customize producer behavior:

| Name| Description | ENV | Default Value  |
| ---|---|---|---|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env if not available in consul | BARITO_KAFKA_BROKERS | localhost:9092 |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ProducerAddressGrpc | gRPC Server Address | BARITO_PRODUCER_GRPC| :8082 |
| ProducerMaxRetry | Set kafka setting max retry | BARITO_PRODUCER_MAX_RETRY | 10 |
| ProducerMaxTps | Producer rate limit trx per second | BARITO_PRODUCER_MAX_TPS | 100 |
| ProducerRateLimitResetInterval | Producer rate limit reset interval (in seconds) | BARITO_PRODUCER_RATE_LIMIT_RESET_INTERVAL | 10 |
| RateLimitType | Rate limiter type (local/redis/gubernator) | BARITO_PRODUCER_RATE_LIMIT_TYPE | local |
| RedisRateLimitAddress | Redis address for rate limiting | BARITO_REDIS_RATE_LIMIT_ADDRESS | localhost:6379 |
| RedisRateLimitPassword | Redis password for rate limiting | BARITO_REDIS_RATE_LIMIT_PASSWORD | |
| RedisRateLimitDb | Redis database number | BARITO_REDIS_RATE_LIMIT_DB | 0 |
| GubernatorAddress | Gubernator server address | BARITO_GUBERNATOR_ADDRESS | |
| PiiRedactionUrl | External PII redaction service URL | BARITO_PII_REDACTION_URL | |
| PiiRedactionFilePath | Path to static PII redaction rules JSON file | BARITO_PII_REDACTION_FILE_PATH | |

## Consumer Mode

Consumer mode is responsible for:

- Consuming logs from Kafka
- Committing logs to Elasticsearch

After the project is built, run:

```sh
./barito-flow Consumer

# or
./barito-flow c
```

### Consumer Configuration

These environment variables can be modified to customize consumer behavior:

| Name| Description | ENV | Default Value  |
| ---|---|----|----|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| ConsulElasticsearchName | Elasticsearch service name in consul | BARITO_CONSUL_ELASTICSEARCH_NAME | elasticsearch |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS| "127.0.0.1:9092,192.168.10.11:9092" |
| KafkaGroupID | kafka consumer group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ElasticsearchUrls | Elasticsearch addresses. Get from env if not available in consul | BARITO_ELASTICSEARCH_URLS | `"http://127.0.0.1:9200,http://192.168.10.11:9200"` |
| EsIndexMethod | BulkProcessor / SingleInsert | BARITO_ELASTICSEARCH_INDEX_METHOD | BulkProcessor |
| EsBulkSize | BulkProcessor bulk size | BARITO_ELASTICSEARCH_BULK_SIZE | 100 |
| EsFlushIntervalMs | BulkProcessor flush interval (ms) | BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS | 500 |
| EsUsername | Elasticsearch username for authentication | BARITO_ELASTICSEARCH_USERNAME | |
| EsPassword | Elasticsearch password for authentication | BARITO_ELASTICSEARCH_PASSWORD | |
| EsCACertPath | Path to CA certificate for mTLS | BARITO_ELASTICSEARCH_CA_CERT_PATH | |
| EsClientCertPath | Path to client certificate for mTLS | BARITO_ELASTICSEARCH_CLIENT_CERT_PATH | |
| EsClientKeyPath | Path to client key for mTLS | BARITO_ELASTICSEARCH_CLIENT_KEY_PATH | |
| PrintTPS | print estimated consumed every second | BARITO_PRINT_TPS | false |
| PushMetricUrl | push metric api url | BARITO_PUSH_METRIC_URL|   |
| PushMetricInterval | push metric interval | BARITO_PUSH_METRIC_INTERVAL | 30s |
| PiiRedactionUrl | External PII redaction service URL | BARITO_PII_REDACTION_URL | |
| PiiRedactionFilePath | Path to static PII redaction rules JSON file | BARITO_PII_REDACTION_FILE_PATH | |

**NOTE**
These following variables will be ignored if `BARITO_ELASTICSEARCH_INDEX_METHOD` is set to `SingleInsert`

- `BARITO_ELASTICSEARCH_BULK_SIZE`
- `BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS`

## Consumer-GCS Mode

Consumer-GCS mode is responsible for:

- Discovering Kafka topics matching a configured regex pattern
- Consuming logs from those topics
- Writing logs to Google Cloud Storage in JSON Lines format

After the project is built, run:

```sh
./barito-flow consumer-gcs

# or
./barito-flow cg
```

### Consumer-GCS Configuration

These environment variables can be modified to customize consumer-GCS behavior:

| Name| Description | ENV | Default Value  |
| ---|---|----|----|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env if not available in consul | BARITO_KAFKA_BROKERS| "127.0.0.1:9092,192.168.10.11:9092" |
| KafkaGroupID | kafka consumer group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| GCPCredentialsPath | Path to GCP service account JSON credentials | BARITO_GCP_CREDENTIALS_PATH | |
| GCSBucketName | GCS bucket name for storing logs | BARITO_GCS_BUCKET_NAME | |
| GCSBufferSize | Number of messages to buffer before flushing to GCS | BARITO_GCS_BUFFER_SIZE | 100 |
| TopicsRegex | Regex pattern to match Kafka topics (e.g., "app-.*") | BARITO_TOPICS_REGEX | |
| PrintTPS | print estimated consumed every second | BARITO_PRINT_TPS | false |

### Changelog

See [CHANGELOG.md](CHANGELOG.md)

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

### License

MIT License, See [LICENSE](LICENSE).
