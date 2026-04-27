# Mastering Flink SQL on Confluent Platform — Lab Materials

Lab infrastructure, data producers, and configuration files for the **Mastering Flink SQL on Confluent Platform** instructor-led training course.

## Directory Structure

```
confluent-flink/
├── scripts/        Setup, health-check, and utility scripts
├── producers/      Data producers (Java + runner scripts)
├── topics/         Kafka topic definitions (CFK YAMLs with AVRO schemas)
├── flink/          Flink catalog, database, and compute pool configs
└── k8s/            Kubernetes manifests (Confluent Platform, S3proxy)
```

## Quick Start

### 1. Full Environment Setup (Lab 1)

Provisions the entire stack: Kind cluster, CFK operator, Confluent Platform (Kafka, Schema Registry, Control Center), cert-manager, Flink Kubernetes Operator, CMF, S3proxy, and Flink resources.

```bash
cd ~/confluent-flink/scripts
bash setup-all.sh
```

Options:

```bash
bash setup-all.sh --skip-kind       # Skip Kind cluster creation (use existing)
bash setup-all.sh --skip-rclone     # Skip rclone + FUSE mount
bash setup-all.sh --teardown        # Delete everything
bash setup-all.sh --name my-cluster # Use a custom Kind cluster name
```

### 2. Health Check & Recovery

Verify the environment is healthy before starting a lab:

```bash
bash scripts/setup-lab.sh              # Check infrastructure (Lab 2)
bash scripts/setup-lab.sh --lab 3      # Also verify Flink resources (Lab 3+)
bash scripts/setup-lab.sh --fix        # Attempt to fix issues automatically
```

### 3. Start a Lab

Creates required topics and starts the appropriate data producer in the background:

```bash
bash scripts/start-lab.sh --lab 3    # Lab 3: orders topic + producer
bash scripts/start-lab.sh --lab 4    # Lab 4: user-actions + clicks topics
bash scripts/start-lab.sh --lab 5    # Lab 5: shoe topics + shoe-producer
bash scripts/start-lab.sh --lab 6    # Lab 6: same as Lab 5
bash scripts/start-lab.sh --stop     # Stop background producers
```

### 4. Port Forwarding

Expose Confluent Platform services on localhost:

```bash
bash scripts/portfw.sh          # Start/restart port forwards
bash scripts/portfw.sh --stop   # Stop port forwards
```

### 5. Stop All Flink Statements

Clean up running Flink SQL statements and zombie deployments:

```bash
bash scripts/stop-all-statements.sh
```

## Service Ports

Once port forwarding is active, services are available at:

| Service          | URL                        |
|------------------|----------------------------|
| CMF UI           | http://localhost:8080       |
| Schema Registry  | http://localhost:8081       |
| Flink REST       | http://localhost:8082       |
| Kafka bootstrap  | localhost:9094              |
| Control Center   | http://localhost:9021       |

## Data Producers

The example domain is an **e-commerce shoe retailer**. Producers generate AVRO-serialized records into Kafka topics:

| Producer | Topics | Runner Script |
|----------|--------|---------------|
| Shoe Producer | `shoe_customers`, `shoe_products`, `shoe_orders` | `producers/shoe-producer/run-shoe-producer.sh` |
| Clicks Producer | `shoe_clickstream` | `producers/clicks-producer/run-clicks-producer.sh` |
| Orders Producer | `orders` | `producers/orders-producer/run-orders-producer.sh` |

## Prerequisites

The training VM comes pre-configured with all required tools:

- `kind`, `kubectl`, `helm`
- `confluent` CLI
- `curl`, `jq`
- Java 11+
