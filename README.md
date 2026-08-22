# Mobility Analytics

A real-time mobility intelligence pipeline built on Apache Kafka, Spark Structured Streaming, Delta Lake, and Hive Metastore. Simulates traffic events, ingests them via Kafka, and validates/enriches them through a bronze-silver-gold pipeline into curated Delta tables for reporting and analytics.

A practical lakehouse implementation for smart transportation analytics — emphasizing streaming data quality, event processing, and dimensional modeling.

---

## 1. Project Overview

Mobility Analytics is a streaming pipeline for traffic monitoring and urban mobility insights — capturing vehicle events, validating/transforming them through layered processing, and producing business-ready tables for dashboards and reporting.

**Answers questions like:**
- Which roads/zones see the most congestion?
- What are peak traffic times?
- Which areas are high-risk or transit-heavy?
- How does weather affect speed and demand?
- Which records are valid, noisy, duplicate, late, or corrupted?

**Demonstrates:** event streaming, schema handling, data quality checks, dimensional modeling, Delta Lake storage, analytical querying



---

## 3. Solution Architecture

The project follows a streaming lakehouse pattern:

1. Data Producer generates synthetic traffic events.
2. Kafka serves as the ingestion backbone.
3. Spark Structured Streaming reads Kafka events.
4. Bronze layer stores raw ingested JSON events in Delta format.
5. Silver layer applies validation, cleansing, deduplication, and enrichment.
6. Gold layer builds dimensions and fact tables for analytics use.
7. Hive Metastore and Delta catalog allow analytical tables to be queried with Spark SQL and Hive-compatible metadata.

High-level flow:

Producer -> Kafka topic -> Bronze Delta -> Silver Delta -> Gold Delta -> BI/Reporting

---

## 4. Tech Stack

**Core:** Python 3, Kafka, Apache Spark 3.5.1, Delta Lake 3.2.0, Hive Metastore, PostgreSQL 13, Docker/Docker Compose, Spark SQL/Hive SQL

**Libraries:** kafka-python, Faker, pytz, PySpark, Delta Lake Spark extension

**Infra:** Kafka (KRaft mode) + Kafka UI, Spark Master/Worker cluster, Hive Metastore on PostgreSQL, local warehouse mount

---




## 6. Data Model

Star-schema model.

**Fact — `fact_traffic`** (1 row/event): `vehicle_id`, `road_id`, `city_zone`, `speed_int`, `congestion_level`, `event_ts`, `peak_flag`, `speed_band`, `hour`, `weather`, `date`

**Dims:**
- `dim_zone`: `city_zone`, `zone_type`, `traffic_risk`
- `dim_road`: `road_id`, `road_type`, `speed_limit`

**Zone risk:** CBD/AIRPORT/TRAINSTATION → high · TECHPARK → medium · SUBURB → low
**Road type:** R100/R200 → highway · others → city

---

## 7. Data Quality Strategy

The producer intentionally injects poor-quality records to test the pipeline's robustness.

### Dirty event patterns
Null/negative/extreme speed, duplicate vehicle IDs, late/future timestamps, wrong datatypes, schema drift, corrupt JSON payloads.

### Silver layer controls
Validates vehicle ID and timestamp presence, detects corrupted payloads, casts speed to integer, enforces valid speed range (0–160) and timestamp logic, dedupes on `vehicle_id` + `event_ts`, watermarks late data.

### Derived fields
`hour`, `peak_flag`, `speed_band`

- LOW: <30
- MEDIUM: 30–69
- HIGH: 70+

---

## 8. Pipeline Details

### 8.1 Producer
Generates synthetic traffic events (Faker-based IDs, random route metadata) every 0.5–1.5s, emitted to Kafka topic `traffic-topic`. Mix: 70% clean, 30% malformed — for realistic ingestion testing.

### 8.2 Bronze Layer
Ingests raw Kafka messages, decodes payloads, parses JSON (flexible schema), writes to Delta table `/warehouse/traffic_bronze` with checkpointing for stream resilience.

### 8.3 Silver Layer
Applies data-quality transforms on the Bronze stream: casts/validates speed and timestamp, flags invalid rows, dedupes on `vehicle_id` + `event_ts`, watermarks late data, derives `hour`, `peak_flag`, `speed_band`. Writes curated output to `/warehouse/traffic_silver`.

### 8.4 Gold Layer
Builds analytics-ready star schema: `dim_zone`, `dim_road`, `fact_traffic` — feeding BI/reporting.

---

## 10. Example Commands

**Install deps**
```bash
pip install kafka-python faker pytz
```

**Create Kafka topic**
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

**Run Bronze / Silver / Gold jobs**
```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_bronze.py
```
*(swap `traffic_bronze.py` → `traffic_silver.py` / `traffic_gold.py` for the other layers)*

**Start Spark SQL with Hive metastore**
```bash
docker exec -it spark-worker bash
mkdir -p /tmp/spark-warehouse && chmod -R 777 /tmp/spark-warehouse
/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse
```
---

## 11. Metadata and Analytics SQL

**Tables:** `mobility.fact_traffic`, `mobility.dim_zone`, `mobility.dim_road`

**BI views:** `bi_fact_traffic`, `bi_dim_zone`, `bi_dim_road` — standardize column names, types, and date fields for downstream BI consistency

**Sample queries:** `COUNT(*)` on `fact_traffic`, previews on `dim_road`/`dim_zone`

---

## 12. Key Numeric Facts

- 1 Kafka topic (`traffic-topic`), 3 partitions
- 1 Spark master, 1 worker (2 CPU cores, 2GB RAM)
- 3 transformation apps → 3 Delta output streams
- 3 gold-layer analytical tables
- 5 traffic zones, 4 road IDs, 4 weather types
- 9 dirty event categories
- 1 Hive metastore DB


---


## 15. How to Run the Project

### Startup steps
1. Start the Docker environment:
```bash
docker-compose up -d
```
2. Create the Kafka topic:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```
3. Install Python dependencies:
```bash
pip install kafka-python faker pytz
```
4. Run the producer script:
```bash
python producer/traffic_dirty_producer.py
```
---



