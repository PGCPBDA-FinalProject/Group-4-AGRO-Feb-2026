# 🌦️ AGRO — India Weather Intelligence Platform

A cloud-native, enterprise-grade weather analytics platform that transforms **14 years of hourly historical weather observations** across **~5,933 geocoded Indian cities** into high-value business insights for **agricultural planning** and **renewable energy investment**. Built on AWS, Apache Spark (PySpark), Amazon Athena, Terraform, and Power BI.

---

## 📌 Overview

Weather volatility directly determines agricultural output and renewable power generation. **AGRO** is a Big Data analytics solution engineered using AWS Medallion Architecture (**Bronze → Silver → Gold**). It processes ~5,933 geocoded Indian cities to unlock actionable data for farmers, agricultural planners, solar/wind developers, and policy makers.

The platform addresses two primary domain challenges:
1. **🌾 Crop Season Shift Detection (PS1)**: Replaces static calendar assumptions with empirical weather detection to compute exact seasonal arrival shifts (early vs. late) and seasonal stability scores across Kharif, Rabi, and Zaid crop cycles.
2. **⚡ Renewable Energy Viability Scoring (PS2)**: Dynamically evaluates 14-year wind speed and solar radiation proxies to produce continuous 0–100 site suitability scores for solar, wind, and hybrid clean energy infrastructure.

---

## 🎯 Problem Statements

### 🌾 PS1 — Agriculture: Season Onset Detection & Shift Analysis
* **Core Challenge**: Traditional agricultural schedules rely on rigid calendar dates. Climate change has shifted onset patterns, resulting in premature sowing, crop failure, or yield loss.
* **Engineered Solution**:
  * Scans historical time-series weather using **data-driven temperature, humidity, and rainfall floors** within dynamic calendar search windows.
  * Quantifies the shift between official calendar start dates and actual detected onset dates in days ($Shift = \text{Detected Start} - \text{Official Start}$).
  * Classifies seasonal arrivals into 4 distinct categories: `Early Arrival`, `On Time`, `Slightly Late`, and `Late Arrival`.
  * Computes **Crop Suitability Score** and **Season Stability Score** at the city, state, and macro-regional level.

### ⚡ PS2 — Renewable Energy: Site Viability & Infrastructure Ranking
* **Core Challenge**: Coarse, bucketed weather scores flatten micro-climate nuances, leading to misallocated capital in solar and wind farm investments.
* **Engineered Solution**:
  * Replaces discrete bucket logic with continuous 0–100 linear min-max normalization for 10m/100m wind speeds and cloud cover.
  * Calculates city-level **Solar Potential Score**, **Wind 10m/100m Scores**, and a weighted **Renewable Index**.
  * Classifies cities into suitability ranks (`Excellent`, `Good`, `Moderate`, `Low`) and automatically recommends optimal energy infrastructure (`Solar`, `Wind`, or `Hybrid`).

---

## 📊 Dataset Specification

* **Source**: Indian Cities Weather Dataset (Kaggle)
* **Scope**: ~5,933 geocoded Indian cities, hourly readings spanning 14 historical years (over tens of millions of weather observations).
* **Key Fields**:
  * **Geospatial & Time**: `city`, `latitude`, `longitude`, `date_time`, `year`, `month`, `day`, `hour`
  * **Atmospheric & Thermal**: `temperature`, `relative_humidity`, `dew_point`, `apparent_temperature`, `pressure_msl`, `surface_pressure`
  * **Precipitation**: `precipitation`, `rain`, `snowfall`, `snow_depth`
  * **Cloud Cover**: `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`
  * **Wind Dynamics**: `wind_speed_10m`, `wind_speed_100m`, `wind_direction_10m`, `wind_direction_100m`, `wind_gusts_10m`
* **Enrichment Datasets**: Indian Historical Crop Yield Dataset & GeoJSON District Boundaries (`india_district.geojson`).

---

## 🏗️ Architecture & Pipeline

<p align="center">
  <img src="Arch.png" width="900" alt="AGRO System Architecture"/>
</p>

### 🔄 Data Pipeline Flow (Medallion Architecture)

```
                     ┌─────────────────────────────────────────┐
                     │          Kaggle Raw Weather Data        │
                     └────────────────────┬────────────────────┘
                                          │
                                          ▼
                     ┌─────────────────────────────────────────┐
                     │       AWS Ingestion / Amazon EMR        │
                     └────────────────────┬────────────────────┘
                                          │
                                          ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ BRONZE LAYER — S3 (s3://agro-weather-data-lake/bronze/)                         │
 │ Raw, immutable, append-only CSV & Parquet storage                               │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ AWS GLUE ETL / EMR (PySpark) — Bronze → Silver Transformation                  │
 │ • Point-in-polygon geospatial enrichment (City → District → State → Region)     │
 │ • Deterministic surrogate key generation (city_id, state_id, region_id)         │
 │ • Early predicate pushdown stratified sampling (30% per state, min 15 cities)   │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ SILVER LAYER — S3 (s3://agro-weather-data-lake2/silver/)                        │
 │ Cleaned Star Schema (dim_region, dim_state, dim_city, fact_weather,             │
 │ fact_weather_sampled) — Parquet format partitioned by state_name                │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ AWS GLUE ETL (PySpark) — Silver → Gold Analytics Engine                         │
 │ • Season Shift Detection Engine (Kharif, Rabi, Zaid algorithm)                  │
 │ • Continuous Renewable Energy Viability Engine (Wind/Solar/Hybrid scoring)      │
 │ • ID-only Fact Tables referencing shared Dimension Tables                       │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ GOLD LAYER — S3 (s3://agro-weather-data-lake3/gold/)                            │
 │ Business-ready analytical models (season_shift, renewable_ranking, daily_weather│
 │ city_profile, ml_dataset) — Partitioned Parquet                                 │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ AWS GLUE CRAWLER & DATA CATALOG (Database: weather_db31)                        │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ AMAZON ATHENA — SQL View Layer (vw_ps1_*, vw_ps3_renewable_dashboard)           │
 └────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │ POWER BI DASHBOARDS — Direct Athena ODBC/JDBC Query Integration                 │
 │ • PS1 Agricultural Season Shift Dashboard                                       │
 │ • PS2 Renewable Energy Potential Dashboard                                      │
 └─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Category | Technology | Usage / Purpose |
|----------|------------|-----------------|
| **Cloud Computing** | **AWS** (EMR, Glue, S3, Athena, IAM, CloudWatch) | Scalable compute, serverless orchestration, data lake storage, and distributed querying |
| **Infrastructure as Code** | **Terraform** | Automated deployment of S3 buckets, Glue jobs, Glue crawlers, and Athena workgroups |
| **Big Data Processing** | **Apache Spark**, **PySpark** (Glue 4.0 / 5.1) | Distributed ETL, spatial joins, time-series aggregations, and window functions |
| **Data Lake Storage** | **Apache Parquet** (Snappy Compression) | Columnar storage partitioned by state for high-performance predicate pushdown |
| **Spatial Processing** | **GeoPandas**, **Shapely**, **Fiona**, **PyProj** | Point-in-polygon mapping of city coordinates into official Indian district boundaries |
| **Query Engine** | **Amazon Athena** | Serverless interactive ANSI SQL execution over S3 Gold data lake |
| **Business Intelligence** | **Power BI Desktop** | Dual interactive dashboards connected via AWS Athena ODBC driver |
| **Version Control & CI/CD** | **Git**, **GitHub Actions** | Source code management, script versioning, and deployment automation |

---

## 💡 Key Engineering Highlights

### 1. Medallion Data Lake Architecture
* **Bronze Layer**: Raw CSV and uncompressed hourly weather data partitioned by source split. Immutable landing zone.
* **Silver Layer**: Standardized star schema (`dim_city`, `dim_state`, `dim_region`, `fact_weather`, `fact_weather_sampled`).
* **Gold Layer**: Domain-specific analytics models (`season_shift`, `renewable_ranking`, `daily_weather`, `city_profile`, `ml_dataset`).

### 2. Spatial Point-in-Polygon & Deterministic Composite Keys
* Performs spatial joins using `GeoPandas` and `Shapely` against official Indian District GeoJSON geometries to assign exact districts, states, and regions.
* Implements a **nearest-neighbor spatial fallback** for coastal or border cities.
* Uses deterministic surrogate hashing (`city_id`, `state_id`, `region_id`) to correctly resolve same-named cities across different Indian states without duplicate collisions.

### 3. Early Stratified Sampling with Predicate Pushdown
> [!TIP]
> **Performance Optimization**:
> Rather than reading and filtering tens of millions of raw weather records, the pipeline performs **stratified percentage sampling (30% per state, min 15 cities)** upfront on `dim_city` metadata. It sets an `is_sampled` boolean flag, enabling **early predicate pushdown** to create a lightweight `fact_weather_sampled` table for rapid dashboard querying.

### 4. Data-Driven Season Detection Algorithm (PS1)
Detects crop season arrival by monitoring environmental windows against calibrated physical thresholds:

$$\text{Shift Days} = \text{Detected Start Date} - \text{Official Start Date}$$

* **Kharif (Monsoon)**: Search window: May 15 – June 15 | Temp: 27–30°C | Rain: $\ge$ 0.20 mm/hr | Humidity: 72–85%
* **Rabi (Winter)**: Search window: Oct 01 – Oct 31 | Temp: 20–25°C | Rain: $\ge$ 0.01 mm/hr | Humidity: 55–70%
* **Zaid (Summer)**: Search window: Mar 01 – Mar 31 | Temp: 28–31°C | Rain: $\ge$ 0.05 mm/hr | Humidity: 45–60%

### 5. Continuous Renewable Scoring Engine (PS2)
To eliminate flat, uninformative bucketed scores in BI visualizations, AGRO implements **continuous 0–100 linear min-max normalization**:

$$\text{Solar Score} = \max(0, 100 - \text{Average Cloud Cover})$$

$$\text{Wind 10m Score} = \min\left(100, \frac{\text{Average Wind Speed 10m}}{20} \times 100\right)$$

$$\text{Wind 100m Score} = \min\left(100, \frac{\text{Average Wind Speed 100m}}{30} \times 100\right)$$

$$\text{Renewable Index} = 0.6 \times \text{Wind 10m Score} + 0.4 \times \text{Solar Score}$$

* **Recommendation Matrix**:
  * `Hybrid`: High solar ($\ge 50$) AND high wind ($\ge 40$)
  * `Solar`: Solar score > Wind score
  * `Wind`: Wind score > Solar score

### 6. ID-Only Gold Fact Tables & Power BI Integrity
* Fact tables in Gold carry **only surrogate IDs** (`city_id`, `state_id`, `region_id`).
* All descriptive attributes (city name, state name, lat/long) reside strictly in dimension tables. This guarantees star schema integrity, prevents relationship ambiguity, and enables cross-filtering in Power BI.

### 7. Automated Data Validation Discipline
The `validation_scripts/validate_silver_layer.py` script enforces strict data quality gates before Gold transformation:
* Orphan key verification (100% referential integrity between fact and dimension tables)
* Grain uniqueness checks on `(city_id, date_time)`
* Null value audit across primary metrics

---

## 📈 Power BI Dashboards & Athena View Layer

The reporting layer uses custom Amazon Athena ANSI SQL views built directly on Gold S3 tables:

### 🌾 PS1 — Agricultural Season Shift Analysis
* **Athena Views**: `vw_ps1_base_clean`, `vw_ps1_arrival_summary`, `vw_ps1_trend`, `vw_ps1_early_arrival`, `vw_ps1_late_arrival`, `vw_ps1_shift_by_season`
* **KPI Metrics**: Maximum Delay (Days), Maximum Advance (Days), Average Season Stability Score, Average Shift Days.
* **Visual Highlights**:
  * State & Regional Season Shift Heatmaps
  * 14-Year Shift Trend Lines by Crop Season (Kharif / Rabi / Zaid)
  * Top 10 Late Arrival Cities vs. Top 5 Early Arrival Cities
  * Interactive Slicers: Region, State, Crop Season, Year

### ⚡ PS2 — Renewable Energy Viability Dashboard
* **Athena Views**: `vw_ps3_renewable_dashboard`
* **KPI Metrics**: Average Renewable Index, Average Solar Score, Average Wind 10m/100m Score.
* **Visual Highlights**:
  * State-Level Renewable Potential Map
  * City-Level Renewable Index Ranking & Suitability Matrix
  * Regional Energy Mix (Solar vs. Wind vs. Hybrid distribution)
  * Interactive Slicers: Region, State, Suitability Rank

---

## 📂 Repository Structure

```
agro/
├── Architecture.jpeg                 # High-level system architecture diagram
├── Arch.png                          # Pipeline schematic diagram
├── README.md                         # Master project documentation
├── bronze to silver.md              # Technical specification for Bronze-to-Silver ETL
├── terraform/                        # Infrastructure as Code (AWS Provisioning)
│   ├── main.tf                       # S3 buckets, Glue ETL jobs, Crawler & Athena setup
│   ├── variables.tf                  # Infrastructure input variables & bucket configurations
│   ├── provider.tf                   # AWS Provider & region settings
│   └── output.tf                     # S3 bucket ARNs, IAM role names, & resource outputs
├── ingestion/                        # Dataset Ingestion Pipelines
│   ├── kaggletos3zip.py              # Automated Kaggle API ZIP download & S3 upload script
│   ├── weather-ingestion.py          # Local weather batch ingestion tool
│   ├── Agro-Ingestion.py             # City hourly weather unzipping & parquet converter
│   ├── glue_weather.py               # AWS Glue job for raw Kaggle weather data ingestion
│   └── glue_crop.py                  # AWS Glue job for crop yield dataset & GeoJSON ingestion
├── emr/                              # Distributed Compute (Amazon EMR PySpark)
│   └── bronze_to_silver_emr.py       # EMR PySpark pipeline (Bronze raw -> Silver Star Schema)
├── gluesilver/                       # AWS Glue PySpark Transformation (Silver Layer)
│   └── bronze_to_silver_glue.py      # Glue PySpark job for geospatial enrichment & sampling
├── gluegold/                         # AWS Glue PySpark Analytics Engine (Gold Layer)
│   └── silver_to_gold_glue.py        # Glue PySpark job for season detection & renewable scoring
├── athena/                           # Amazon Athena SQL Layer
│   └── athena.sql                    # SQL DDL & analytical view definitions for PS1 (vw_ps1_*)
├── dataVisualization/                # Power BI Reporting Layer & SQL Integration
│   ├── View.sql                      # SQL View DDL for PS2 (vw_ps3_renewable_dashboard)
│   └── DataVisualization_powerBi-ConnectionFlow.docx  # Power BI setup & connection guide
├── validation_scripts/               # Data Quality & Governance Framework
│   └── validate_silver_layer.py      # Data quality validator & referential integrity auditor
├── scripts/                          # Utilities & EMR Cluster Helpers
│   ├── bootstrap.sh                  # EMR cluster initialization script (GeoPandas/Shapely dependencies)
│   ├── geojson.py                    # GeoJSON boundary fetcher & validator
│   ├── convert_city_master.py        # City coordinate master lookup generator
│   ├── convert_wd1.py                # Weather partition split converter (Part 1)
│   ├── convert_wd2.py                # Weather partition split converter (Part 2)
│   ├── convert_wd3.py                # Weather partition split converter (Part 3)
│   └── steps.json                    # EMR step execution parameters
└── notebooks/                        # Exploratory Data Analysis & Prototyping
    ├── eda1.ipynb                    # Weather distribution & missing value analysis
    ├── weatherDatasetEDA (3) (1).ipynb # 14-year time series exploratory analysis
    ├── notebook_vaishnavi/           # PS1 Season Shift experimental notebooks
    ├── notebooks_pravin/             # PS2 Renewable Energy preliminary scoring models
    ├── notebooks_shreyansh/          # Gold layer aggregation experiments
    ├── notebooks_vishal/             # Wind & Solar metric distribution analysis
    ├── transformation_exp/           # Silver transformation prototyping
    └── transformation_gold_exp/      # Gold transformation prototyping
```

---

## 🚀 Deployment & Execution Guide

### Prerequisites
* AWS Account with Admin access to S3, Glue, EMR, Athena, and IAM.
* Terraform CLI (v1.5+) installed.
* Python 3.9+ & PySpark installed locally (optional, for validation).
* Kaggle API credentials (`kaggle.json`).

---

### Step 1: Provision Infrastructure via Terraform
```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```
*This provisions S3 buckets (`bronze`, `silver`, `gold`, `athena-results`), AWS Glue Database (`weather_db31`), Glue Jobs, IAM Roles, and Athena Workgroups.*

---

### Step 2: Ingest Raw Data into Bronze S3 Layer
Run the ingestion pipeline to fetch Kaggle weather dataset & Indian district GeoJSON boundaries:
```bash
python ingestion/kaggletos3zip.py
```
*Alternatively, trigger the AWS Glue ingestion jobs provisioned by Terraform:*
```bash
aws glue start-job-run --job-name weather_ingestion_bronze
aws glue start-job-run --job-name crop_geojson_ingestion_bronze
```

---

### Step 3: Execute Bronze → Silver PySpark ETL
#### Option A: Run via AWS Glue (Recommended)
```bash
aws glue start-job-run --job-name bronze_to_silver_transformation
```

#### Option B: Run via Amazon EMR Cluster
1. Launch EMR Cluster using `scripts/bootstrap.sh` to install `geopandas`, `shapely`, `fiona`, and `pyproj`.
2. Submit EMR Step:
```bash
aws emr add-steps --cluster-id <YOUR_CLUSTER_ID> --steps file://scripts/steps.json
```

---

### Step 4: Run Data Quality Audit
Validate referential integrity and schema compliance before proceeding to Gold transformation:
```bash
python validation_scripts/validate_silver_layer.py --silver-bucket s3://agro-weather-data-lake2/silver
```

---

### Step 5: Execute Silver → Gold Analytics Pipeline
Run the Gold Layer transformation job to compute season shifts and renewable scores:
```bash
aws glue start-job-run --job-name golden_layer
```

---

### Step 6: Catalog & Create Athena SQL Views
1. Run the Glue Crawler to populate Glue Data Catalog (`weather_db31`):
```bash
aws glue start-crawler --name weather_gold_crawler
```
2. Execute the Athena View SQL scripts in Amazon Athena Console:
   * Execute `athena/athena.sql` (Creates PS1 views: `vw_ps1_base_clean`, `vw_ps1_arrival_summary`, `vw_ps1_trend`, etc.)
   * Execute `dataVisualization/View.sql` (Creates PS2 view: `vw_ps3_renewable_dashboard`)

---

### Step 7: Connect Power BI to Amazon Athena
1. Open Power BI Desktop and select **Get Data → Amazon Athena**.
2. Connect using the Athena ODBC DSN configured for region & `s3://athena-results-bucket/`.
3. Load views `vw_ps1_base_clean` and `vw_ps3_renewable_dashboard` to interact with pre-calculated PS1 & PS2 analytical visual reports.

---

## 👥 Team & Contributions

| Team Member | Domain & Engineering Responsibilities |
|-------------|────────────────────────────────────────|
| **Shreyansh** | Project Architecture Lead · Silver-to-Gold Pipeline (`silver_to_gold_glue.py`) · Continuous Renewable Scoring Engine |
| **Parigha** | Data Ingestion Lead · Kaggle API Automation & EMR Bootstrap Orchestration (`kaggletos3zip.py`, `glue_weather.py`) |
| **Krishna** | Bronze-to-Silver Transformation Lead · GeoSpatial Point-in-Polygon Engine & Stratified Sampling (`bronze_to_silver_glue.py`, `bronze_to_silver_emr.py`) |
| **Swapnil** | Infrastructure as Code & Automation Lead · Terraform Modules & IAM Governance (`terraform/main.tf`) |
| **Vaishnavi** | PS1 Agricultural Analytics Lead · PS1 Season Shift Dashboard & Athena View Layer (`athena/athena.sql`) |
| **Vishal & Pravin** | PS2 Renewable Energy Analytics Lead · PS2 Viability Dashboard & Renewable Index Views (`dataVisualization/View.sql`) |

---

## 🔮 Future Enhancements

* **ML-Based Season Onset Prediction**: Implement PySpark MLlib (Random Forest / XGBoost) to forecast next year's crop season arrival dates based on historical climate vectors.
* **Monsoon Synchronization Analysis**: Regional cross-correlation analysis of rainfall pattern synchronization across neighboring states.
* **Climate-Based City Clustering**: Unsupervised K-Means clustering of Indian cities into distinct agro-climatic zones based on long-term weather profiles.
* **Crop Recommendation Engine**: Machine learning model recommending optimal crop varieties based on predicted season shifts and soil suitability.
* **Real-Time Streaming Ingestion**: Integration of AWS Kinesis / Spark Streaming for real-time weather API integration and live alert triggering.

---

## 📜 License

This project is developed for educational and research purposes. All dataset rights belong to the original Kaggle publishers.
