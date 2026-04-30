# Lakehouse Prototype
## Overview

This project implements a sovereign, open-source lakehouse prototype designed for hybrid batch and streaming ingestio with strong governance.

The architecture is based on:

* **Kafka** for streaming ingestion
* **Schema Registry** for schema validation and compatibility
* **Kafka topics as Bronze/Silver persistence layers**
* **Apache Spark** for transformations and processing
* **Apache Iceberg** as the table format (future integration)
* **Ceph Object Storage** as sovereign storage (future integration)
* **Nessie Catalog** as metadata/control plane (future integration)

The current prototype focuses on validating the ingestion backbone and governance-first pipeline design before integrating full lakehouse persistence.


# Architecture Flow

```text
Data Sources
     ↓
Kafka Producers
     ↓
Schema Registry for schema validation
     ↓
Kafka Bronze Topics
     ↓
Spark for business rules
     ↓
Kafka Silver Topics

---

# Current Implemented Components

## 1. Kafka Streaming Backbone

Kafka acts as the ingestion bus.

Each producer publishes domain-specific events.

Example domains:

* sales
* logistics
* iot

---

## 2. Schema Registry

Schema Registry ensures:

* Producer payload validation
* Schema evolution compatibility
* Strong contracts between producers and consumers

Each Kafka topic has an associated schema.

Example:

```text
bronze.sales.customers-value
bronze.sales.products-value
bronze.sales.transactions-value
```

---

## 3. Producers

Each producer:

1. Generates or reads source data
2. Validates payload against Avro schema
3. Serializes payload
4. Publishes event to Kafka topic


# Governance Strategy

Governance is implemented early in the ingestion lifecycle.

Shift-left governance includes:

* Schema validation
* Data quality checks
* Topic separation
* Quarantine topics
* Versioned contracts

This follows modern shift-left principles for data pipelines, where quality and validation happen near the source rather than downstream.
