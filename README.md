# Real-Time Uber Medallion Lakehouse Engineering

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakeflow%20%2F%20SDP-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure Event Hubs](https://img.shields.io/badge/Azure-Event%20Hubs%20(Kafka)-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Jinja2](https://img.shields.io/badge/Templating-Jinja2-B41717?logo=python&logoColor=white)](https://palletsprojects.com/p/jinja/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?logo=delta&logoColor=white)](https://delta.io/)

An end-to-end, production-grade real-time streaming data pipeline simulating an on-demand ride-hailing platform (Uber). Built using **Azure Event Hubs (Kafka protocol)**, **Azure Data Factory**, **PySpark Structured Streaming**, and **Spark Declarative Pipelines (SDP / Lakeflow)** on Databricks Unity Catalog.

---

## Architecture Overview

[FastAPI Web App] ──(Produce Ride Events)──> [Azure Event Hubs (Kafka)]
│
(Spark ReadStream)
▼
[GitHub / HTTP APIs] ──(ADF ForEach Copy)──> [ADLS Gen2 Ingest]
│
(Spark Batch Read)
▼
═══════════════════════════════════════════════════════════════════════
UNITY CATALOG (uber)
═══════════════════════════════════════════════════════════════════════
BRONZE LAYER:
• uber.bronze.rides_raw         (Real-Time Kafka Event Ingestion)
• uber.bronze.bulk_rides        (Historical Lift & Shift - 2,000+ rides)
• uber.bronze.map_*             (6 Static Mapping/Lookup Delta Tables)
• uber.bronze.stg_rides         (Idempotent Append Flow: Stream + Bulk)
│
(3-min Watermark Join)
(Jinja2 Dynamic OBT)
▼
SILVER LAYER:
• uber.silver.silver_obt        (Consolidated One Big Table)
│
(create_auto_cdc_flow)
▼
GOLD LAYER (Star Schema):
• uber.gold.fact                (Central Ride Metrics & Dimensional Keys)
• uber.gold.Dim_passenger       (SCD Type 1)
• uber.gold.Dim_driver          (SCD Type 1)
• uber.gold.Dim_vehicle         (SCD Type 1)
• uber.gold.Dim_payments        (SCD Type 1)
• uber.gold.Dim_booking         (SCD Type 1 - Master Booking Context)
• uber.gold.Dim_location        (SCD Type 2 - Historical Demographic Tracking)
═══════════════════════════════════════════════════════════════════════


---

## Core Engineering Features

### 1. Unified Real-Time Streaming & Historical Backfill
* **Kafka-on-Event Hubs**: Ingests real-time ride confirmation JSON payloads via Kafka SASL/SSL protocol directly into `uber.bronze.rides_raw`.
* **Append Flow Idempotency**: Employs `@dp.append_flow` to seamlessly unify continuous streaming events with 2,000+ historical batch records (`bulk_rides`) into `uber.bronze.stg_rides` without duplicating records upon pipeline restarts.

### 2. Metadata-Driven Ingestion (Zero-Code Maintenance)
* **ADF Dynamic Copy Pipeline**: Replaces repetitive data pipelines with a dynamic ADF workflow. A central `files_array.json` catalog feeds an ADF `Lookup` and `ForEach` activity, automatically parameterizing source endpoints and sinking all reference files into ADLS Gen2.
* **Jinja2 SQL Compilation for Silver OBT**: Implements a modular SQL generator where joining conditions, source tables, and column projections are maintained in a Python configuration dictionary. Jinja2 compiles production-grade SQL with automated alias resolution, null-safe comma handling, and join logic on the fly.

### 3. Stateful Streaming Joins with Watermarking
* Joins the real-time ride event stream (`stg_rides`) against static Delta lookup tables (`map_cities`, `map_vehicle_types`, etc.).
* Implements `WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES` to bound engine memory state, safely discard excessively late events, and maintain high-throughput processing.

### 4. Declarative Auto-CDC & Slowly Changing Dimensions
* **SCD Type 1 (In-Place Updates)**: Applied to Passenger, Driver, Vehicle, Payments, and Booking dimensions to maintain the latest state.
* **SCD Type 2 (Historical Tracking)**: Implemented on `uber.gold.Dim_location` sequenced by `city_updated_at`. When municipal boundaries or demographic records change, the engine closes out the active record with a timestamp in `__END_AT` and inserts the updated record with `__END_AT = NULL`.
* **Cartesian Explosion Prevention**: All dimensional queries and fact table joins explicitly enforce `WHERE dim.end_at IS NULL` to prevent row multiplication against historical records.

---

## Repository Structure

```text
├── app/
│   ├── api.py                     # FastAPI web application for booking simulation
│   ├── data.py                    # Synthetic ride payload generator
│   └── connection.py              # Azure Event Hubs client producer
├── databricks/
│   ├── 01_bronze/
│   │   ├── ingest.py              # Streaming ingestion from Event Hub to bronze
│   │   └── bronze_adls.py         # Batch ingestion of lookup tables from ADLS
│   ├── 02_silver/
│   │   ├── silver.py              # Staging table append flows (bulk + stream)
│   │   ├── jinja_obt_compiler.py  # Jinja2 metadata-driven SQL compiler
│   │   └── silver_obt.sql         # Silver OBT definition with watermark
│   └── 03_gold/
│       └── model.py               # Gold star schema dimensions and fact with Auto-CDC

