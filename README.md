# E-Commerce Clickstream & Inventory Watch Pipeline

A production-grade real-time data pipeline built on the **Kappa Architecture** for an online electronics store. The system ingests clickstream events, processes them with sliding-window aggregations, triggers dynamic pricing recommendations, and generates daily analytics through an orchestrated batch layer.

---

## Architecture

![Kappa Architecture Diagram](architecture/kappa_architecture.drawio.png)

```
Clickstream Simulator ──► Kafka ──► Spark Structured Streaming ──► PostgreSQL
                                          │                              │
                                          ▼                              ▼
                                   Flash Sale Trigger           Apache Airflow
                                   (> 100 views, < 5 purchases)      │
                                                               ┌──────┴──────┐
                                                               ▼             ▼
                                                      User Segmentation  Daily Reports
                                                      (Window Shoppers   (Top-5 Products,
                                                        vs Buyers)        Conversion Rates)
```

**Architecture Pattern:** Kappa — a single unified stream-processing layer eliminates the dual-layer complexity of Lambda. Airflow orchestrates periodic queries against the live storage layer rather than maintaining a separate batch computation path.

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka | 7.5.0 (Confluent) |
| Stream Processor | Apache Spark Structured Streaming | 3.4.1 |
| Orchestration | Apache Airflow | 2.8.1 |
| Storage | PostgreSQL | 15 |
| Producer | Python (kafka-python) | 3.11 |
| Infrastructure | Docker Compose | v3.8 |

---

## Repository Structure

```
pipeline/
├── architecture/
│   ├── kappa_architecture.drawio        # Architecture diagram source (draw.io)
│   └── kappa_architecture.drawio.png    # Exported architecture diagram
├── config/
│   ├── kafka_config.py                  # Kafka connection constants
│   ├── spark_config.py                  # Spark job configuration
│   └── init_db.sql                      # PostgreSQL schema initialisation
├── producer/
│   ├── clickstream_producer.py          # Kafka event simulator
│   └── requirements.txt
├── spark/
│   ├── streaming_job.py                 # Main Spark Streaming job
│   ├── flash_sale_trigger.py            # Flash sale detection & alert dispatcher
│   └── requirements.txt
├── airflow/
│   ├── dags/
│   │   ├── user_segmentation_dag.py     # Daily user segmentation DAG
│   │   └── daily_report_dag.py          # Daily top-5 products report DAG
│   └── requirements.txt
├── reports/
│   ├── conversion_rate_report.py        # Conversion rate analytics generator
│   └── output/                          # Generated report artefacts
├── docs/
│   └── project_report.md               # 1 500-word project report
├── docker-compose.yml                   # Full stack orchestration
└── README.md
```

---

## Prerequisites

- Docker Desktop ≥ 24.0
- Docker Compose ≥ 2.20
- Python 3.11 (for local development outside Docker)

---

## Quick Start

### 1. Start the full stack

```bash
docker compose up -d
```

Services exposed:

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8085 |
| Spark Master UI | http://localhost:8080 |
| Airflow Webserver | http://localhost:8082 (admin / admin) |
| PostgreSQL | localhost:5432 |

### 2. Run the clickstream producer

```bash
cd producer
pip install -r requirements.txt
python clickstream_producer.py
```

### 3. Submit the Spark Streaming job

```bash
docker exec ecommerce_spark_master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
  /opt/bitnami/spark/jobs/streaming_job.py
```

### 4. Trigger Airflow DAGs

Navigate to http://localhost:8082, enable and manually trigger:
- `user_segmentation`
- `daily_product_report`

### 5. Generate the conversion rate report

```bash
cd reports
pip install -r requirements.txt
python conversion_rate_report.py
```

---

## Business Logic

### Flash Sale Trigger

The Spark Streaming job maintains a **10-minute sliding window** (slide interval: 2 minutes) per product. When a product exceeds **100 views** with **fewer than 5 purchases** within that window, a `FLASH_SALE_CANDIDATE` alert is written to `flash_sale_alerts`.

### User Segmentation

Airflow runs `@daily` to classify every active user:

| Segment | Criteria |
|---------|----------|
| `Buyer` | At least one `purchase` event |
| `Prospective_Buyer` | At least one `add_to_cart`, no purchase |
| `Window_Shopper` | Only `view` events |

### Conversion Rate

`purchases / views × 100` computed per product and per category across the daily reporting window.

---

## Reports

All generated artefacts land in `reports/output/`:

| File | Description |
|------|-------------|
| `top_products_<date>.txt` | Top 5 most viewed products with conversion rates |
| `conversion_rates_<date>.csv` | Full per-product conversion rate breakdown |

---

## Stopping the Stack

```bash
docker compose down -v
```