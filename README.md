# Lakehouse Kafka + Spark Shift-Left Prototype

im thinking of an ai agent to automate the translation of rules in data contrcat to actual spark jobs, so whe a new data source comes and a new data contracts associated to it, no need to wait for an engineer to undeetand thew data source and schema and rules to make the corresponding sspark job for tranformstion n validation, we could jsut use an agent to do so.

do we do a one python file for all producers ??
This prototype simulates the first step toward a sovereign lakehouse:

Producer → Kafka bronze → Spark validation/transformation → Kafka silver / DLQ

Kafka silver is temporary. Later, it will be replaced or complemented by Iceberg tables on Ceph, registered through Nessie.

## 1. Start infrastructure

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8080
```

## 2. Install Python producer dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
## 3. Start producers

Open three terminals:

```bash
python producers/sales_producer.py
```

```bash
python producers/logistics_producer.py
```

```bash
python producers/sensor_producer.py
```

## 4. Start Spark validation job

```bash
docker exec -it spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark_jobs/validate_bronze_to_silver.py
```

## 5. Observe topics in Kafka UI

Bronze topics:

```text
bronze.sales.transactions
bronze.logistics.shipments
bronze.sensors.events
```

Silver topics:

```text
silver.sales.transactions
silver.logistics.shipments
silver.sensors.events
```

DLQ topics:

```text
dlq.sales.transactions
dlq.logistics.shipments
dlq.sensors.events
```

## Interpretation

- Producers simulate domain systems.
- Bronze topics represent raw but ingested data.
- Contracts are stored in `/contracts`.
- Spark Structured Streaming executes contract rules.
- Valid records go to silver topics.
- Invalid records go to DLQ topics.
