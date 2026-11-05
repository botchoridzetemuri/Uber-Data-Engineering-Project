# Real-Time Uber Medallion Lakehouse Engineering

![Azure](https://img.shields.io/badge/Azure-Event%20Hubs%20%7C%20ADF%20%7C%20ADLS%20Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Lakeflow%20%2F%20SDP-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20%26%20Time%20Travel-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Live%20Simulation-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Jinja2](https://img.shields.io/badge/Jinja2-Metadata--Driven%20SQL-B41717?style=for-the-badge&logo=python&logoColor=white)

An enterprise-grade, end-to-end real-time data engineering platform simulating an on-demand ride-hailing system (Uber). The pipeline captures client-side ride dispatch events in real time, unifies them with static reference metadata and historical backfills, dynamically compiles SQL transformations using Jinja2, and materializes a production Star Schema with Auto-CDC Slowly Changing Dimensions (SCD 1 & 2) in Databricks Unity Catalog.

---

## Architecture Blueprint

![System Architecture](Images/architecture.png)

### End-to-End Pipeline Execution DAG (Databricks Lakeflow)

![Databricks Pipeline Graph](Images/Pipeline_Graph.png)

---

## Executive Summary & Engineering Challenges

High-concurrency mobility platforms process millions of transaction events every minute—dispatches, real-time GPS telemetry, surge calculations, and fare reconciliations. Operating teams need sub-minute analytical visibility without compromising data consistency or blowing cloud compute budgets.

### Core Problems Solved:
1. **Unifying Streaming Events with Massive Historical Backfills:**
   * *Problem:* Ingesting live firehose transactions while reconciling 2,000+ historical batch records (`bulk_rides`) often introduces duplicate records or requires expensive repeated full-table overwrites.
   * *Solution:* Leveraged Databricks Spark Declarative Pipelines (SDP / Lakeflow) with `@dp.append_flow`. The historical backfill runs once (`once=True`), while streaming transactions append continuously (`once=False`), ensuring strictly idempotent, exactly-once processing.

2. **Eliminating Pipeline Maintenance Debt via Metadata-Driven Ingestion:**
   * *Problem:* Hardcoding schemas for 7+ reference dimension lookups across pipelines creates brittle code that breaks whenever upstream schema attributes change.
   * *Solution:* Developed two levels of dynamic automation:
     * **Azure Data Factory:** A single parameterized pipeline iterates over a dynamic configuration array (`files_array.json`), automating HTTP-to-ADLS Gen2 copy activities without manual pipeline duplication.
     * **Jinja2 SQL Compilation Engine:** Dynamically generates complex multi-table joins for the Silver One Big Table (OBT) from a Python configuration dictionary, reducing schema evolution maintenance to zero-code configuration edits.

3. **Bounding State Memory on Real-Time Joins:**
   * *Problem:* Joining continuous transaction streams with static dimensions can cause memory state explosion if late-arriving events are not bounded.
   * *Solution:* Applied an explicit 3-minute watermark (`WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES`) to drop stale states outside the processing window, safeguarding cluster stability.

4. **Tracking Historical Attribute Drift (SCD Type 2):**
   * *Problem:* Reference dimensions (e.g., city territory reallocations) change over time. Overwriting values corrupts historical reporting, while standard tracking requires hundreds of lines of complex merge scripts.
   * *Solution:* Implemented `dp.create_auto_cdc_flow` to automatically manage `__START_AT` and `__END_AT` audit intervals on `Dim_location`, while enforcing strict active-record filters (`__END_AT IS NULL`) on fact table joins to prevent Cartesian fan-out.

---

## Medallion Architecture Breakdown

[ FastAPI Booking App ] ───> [ Azure Event Hubs (Kafka) ] ───> [ uber.bronze.rides_raw ]
│
[ GitHub / HTTP Lookups ] ──> [ Azure Data Factory ] ───────> [ uber.bronze.map_* ]
│
▼
[ uber.bronze.stg_rides ]
(Append Flow: Bulk + Stream)
│
(3-Min Watermark Join)
(Jinja2 OBT Engine)
▼
[ uber.silver.silver_obt ]
│
(dp.create_auto_cdc)
▼
═════════════════════════════════
GOLD LAYER (Star Schema)
═════════════════════════════════
• uber.gold.fact (Central Fact)
• uber.gold.Dim_location (SCD 2)
• uber.gold.Dim_passenger (SCD 1)
• uber.gold.Dim_driver (SCD 1)
• uber.gold.Dim_vehicle (SCD 1)
• uber.gold.Dim_payments (SCD 1)
• uber.gold.Dim_booking (SCD 1)
---

## Detailed Pipeline Implementation

### 1. The Producer: Live Simulation App (`App/`)
* **Local Web Interface:** Built a FastAPI web application (`App/api.py`) backed by HTML templates (`App/app_design/`) allowing interactive ride booking from any browser.
* **Telemetry Generator (`App/data.py`):** Programmatically constructs real-time JSON payloads containing ride IDs, confirmation numbers, dynamic GPS coordinates, driver metadata, surge multipliers, fares, and timestamps.
* **Event Hubs Kafka Producer (`App/connection.py`):** Transmits payloads asynchronously over SASL/SSL directly into Azure Event Hubs (`ubertopic`) with full Kafka protocol compatibility.

### 2. Bronze Layer: Raw Ingestion & Staging (`Databricks/.../bronze/`)
* **`uber.bronze.rides_raw`:** Reads Kafka streams via `spark.readStream.format("kafka")`, converting raw binary payloads into JSON strings for auditing and replayability.
* **`uber.bronze.bulk_rides` & Lookup Tables:** Batch loads 2,000+ historical rides along with 6 static lookup tables (`map_cities`, `map_vehicle_types`, `map_vehicle_makes`, `map_payment_methods`, `map_ride_statuses`, `map_cancellation_reasons`) into Delta format.
* **`uber.bronze.stg_rides`:** Combines historical batch records with parsed, schema-validated streaming transactions using `@dp.append_flow`.

### 3. Silver Layer: Dynamic OBT & Stateful Streaming Joins (`Databricks/.../silver/`)
* **Jinja2 SQL Templating Engine:** Automatically constructs the multi-table join SQL. Instead of hardcoded statements, dimension additions are maintained in a configuration dictionary:
  ```python
  {
      "table": "uber.bronze.map_vehicle_types",
      "select": "map_vehicle_types.vehicle_type, map_vehicle_types.base_rate, map_vehicle_types.per_mile",
      "on": "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
  }
Watermarked Stream-Static Join (Silver_OBT.sql): Combines the live transaction stream with all reference tables to produce the consolidated One Big Table (uber.silver.silver_obt):SQLCREATE OR REFRESH STREAMING TABLE uber.silver.silver_obt AS
SELECT 
    stg_rides.*,
    map_cities.city AS pickup_city,
    map_cities.state,
    map_cities.region,
    map_cities.updated_at AS city_updated_at,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type,
    map_payment_methods.payment_method
FROM stream(uber.bronze.stg_rides)
WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
LEFT JOIN uber.bronze.map_cities ON stg_rides.pickup_city_id = map_cities.city_id
LEFT JOIN uber.bronze.map_vehicle_types ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
...
4. Gold Layer: Star Schema & Automated CDC (Databricks/.../gold/model.py)Materializes a normalized Dimensional Model partitioned into 6 dimensions and a central Fact table:TableModeling StrategyPrimary KeySequencing ColumnPurposeDim_passengerSCD Type 1passenger_idpassenger_idPassenger profiles and contact informationDim_driverSCD Type 1driver_iddriver_idDriver credentials, ratings, and licensingDim_vehicleSCD Type 1vehicle_idvehicle_idVehicle specifications, models, and classesDim_paymentsSCD Type 1payment_method_idpayment_method_idPayment gateway configurations and auth flagsDim_bookingSCD Type 1ride_idride_idMaster booking context and routing endpointsDim_locationSCD Type 2pickup_city_idcity_updated_atTracks municipal boundary and regional updatesfactTransaction FactComposite Keysride_idCore numeric metrics (fares, distance, tip, surge)Analytical Verification Queries1. Active Dimensional Star Schema QuerySQLSELECT 
    f.ride_id,
    p.passenger_name,
    d.driver_name,
    v.vehicle_model,
    loc.pickup_city,
    f.distance_miles,
    f.duration_minutes,
    f.surge_multiplier,
    f.total_fare
FROM uber.gold.fact f
LEFT JOIN uber.gold.Dim_passenger p 
    ON f.passenger_id = p.passenger_id
LEFT JOIN uber.gold.Dim_driver d 
    ON f.driver_id = d.driver_id
LEFT JOIN uber.gold.Dim_vehicle v 
    ON f.vehicle_id = v.vehicle_id
LEFT JOIN uber.gold.Dim_location loc 
    ON f.pickup_city_id = loc.pickup_city_id 
    AND loc.__END_AT IS NULL   -- Guardrail: Filters out expired historical records
LIMIT 10;
2. Auditing SCD Type 2 Historical ChangesSQLSELECT 
    pickup_city_id,
    pickup_city,
    city_updated_at,
    __START_AT,
    __END_AT
FROM uber.gold.Dim_location
WHERE pickup_city_id = 2
ORDER BY __START_AT ASC;
Tech Stack SummaryEvent Broker: Azure Event Hubs (Standard Tier, Kafka Protocol)Storage Layer: Azure Data Lake Storage Gen2 (Hierarchical Namespace) & Delta LakeOrchestration: Azure Data Factory (Dynamic Parameterized Pipelines) & Databricks WorkflowsTransformation & Processing: Apache Spark 3.5+, PySpark Structured Streaming, Spark Declarative Pipelines (Lakeflow / DLT)Data Modeling & Governance: Databricks Unity Catalog, Jinja2 Templating, Auto-CDC (SCD 1 & 2)Producer Simulation: Python, FastAPI, Uvicorn, Jinja2

