-- =============================================================================
-- Pipeline database initialisation
-- Creates the pipeline schema alongside the airflow metadata database.
-- Executed automatically by the postgres Docker container on first start.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Databases
-- ---------------------------------------------------------------------------
CREATE USER pipeline WITH PASSWORD 'pipeline';

CREATE DATABASE pipeline OWNER pipeline;
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
ALTER DATABASE airflow OWNER TO airflow;

-- ---------------------------------------------------------------------------
-- Connect to the pipeline database for all subsequent DDL
-- ---------------------------------------------------------------------------
\connect pipeline

SET search_path TO public;

-- ---------------------------------------------------------------------------
-- Raw clickstream events
-- Stores every event produced by the clickstream simulator.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream_events (
    id            BIGSERIAL    PRIMARY KEY,
    user_id       VARCHAR(64)  NOT NULL,
    product_id    VARCHAR(64)  NOT NULL,
    category      VARCHAR(64),
    event_type    VARCHAR(16)  NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    event_time    TIMESTAMPTZ  NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ce_product_event_time
    ON clickstream_events (product_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_ce_user_id
    ON clickstream_events (user_id);

-- ---------------------------------------------------------------------------
-- Windowed product view aggregations (written by Spark)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_view_aggregates (
    id              BIGSERIAL    PRIMARY KEY,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    product_id      VARCHAR(64)  NOT NULL,
    view_count      BIGINT       NOT NULL DEFAULT 0,
    cart_count      BIGINT       NOT NULL DEFAULT 0,
    purchase_count  BIGINT       NOT NULL DEFAULT 0,
    computed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, window_end, product_id)
);

-- ---------------------------------------------------------------------------
-- Flash-sale alerts (written by the flash_sale_trigger job)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flash_sale_alerts (
    id              BIGSERIAL    PRIMARY KEY,
    product_id      VARCHAR(64)  NOT NULL,
    view_count      BIGINT       NOT NULL,
    purchase_count  BIGINT       NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    alert_message   TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- User segmentation results (written by Airflow DAG)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_segments (
    id              BIGSERIAL    PRIMARY KEY,
    segment_date    DATE         NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    segment         VARCHAR(32)  NOT NULL CHECK (segment IN ('Buyer', 'Window Shopper')),
    total_views     BIGINT       NOT NULL DEFAULT 0,
    total_purchases BIGINT       NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (segment_date, user_id)
);

-- ---------------------------------------------------------------------------
-- Daily product report (written by Airflow DAG)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_product_report (
    id              BIGSERIAL    PRIMARY KEY,
    report_date     DATE         NOT NULL,
    rank            SMALLINT     NOT NULL,
    product_id      VARCHAR(64)  NOT NULL,
    total_views     BIGINT       NOT NULL,
    total_purchases BIGINT       NOT NULL,
    conversion_rate NUMERIC(6,4) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (report_date, rank)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline;
