import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from config.spark_config import (
    APP_NAME,
    FLASH_SALE_MAX_PURCHASES,
    FLASH_SALE_MIN_VIEWS,
    KAFKA_SOURCE_OPTIONS,
    PG_DB,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    SLIDE_DURATION,
    SPARK_MASTER,
    SPARK_PACKAGES,
    WATERMARK_DELAY,
    WINDOW_DURATION,
)

# ---------------------------------------------------------------------------
# Schema of the JSON payload produced by clickstream_producer.py
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("referrer", StringType(), True),
    ]
)

POSTGRES_CONN_KWARGS = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": PG_DB,
    "user": PG_USER,
    "password": PG_PASSWORD,
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(APP_NAME)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_SOURCE_OPTIONS)
        .load()
        .select(
            F.from_json(
                F.col("value").cast("string"), EVENT_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            F.to_timestamp("data.timestamp").alias("event_time"),
        )
    )


def aggregate_windows(stream):
    """
    Sliding-window aggregation: views, add-to-carts, and purchases
    per product over WINDOW_DURATION with SLIDE_DURATION step.
    Uses event-time with WATERMARK_DELAY tolerance for late data.
    """
    return (
        stream.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("product_id"),
        )
        .agg(
            F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("cart_count"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("product_id"),
            F.col("view_count"),
            F.col("cart_count"),
            F.col("purchase_count"),
            F.current_timestamp().alias("computed_at"),
        )
    )


def execute_upsert(query: str, rows: List[Tuple]) -> None:
    if not rows:
        return

    connection = psycopg2.connect(**POSTGRES_CONN_KWARGS)
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            execute_values(cursor, query, rows, page_size=500)
        connection.commit()
    finally:
        connection.close()


def write_aggregates_to_postgres(agg_stream):
    """Upsert each micro-batch of aggregated windows into PostgreSQL."""

    def upsert_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        rows = [
            tuple(row)
            for row in batch_df.select(
                "window_start",
                "window_end",
                "product_id",
                "view_count",
                "cart_count",
                "purchase_count",
                "computed_at",
            ).collect()
        ]
        execute_upsert(
            """
            INSERT INTO product_view_aggregates (
                window_start,
                window_end,
                product_id,
                view_count,
                cart_count,
                purchase_count,
                computed_at
            )
            VALUES %s
            ON CONFLICT (window_start, window_end, product_id)
            DO UPDATE SET
                view_count = EXCLUDED.view_count,
                cart_count = EXCLUDED.cart_count,
                purchase_count = EXCLUDED.purchase_count,
                computed_at = EXCLUDED.computed_at
            """,
            rows,
        )

    return (
        agg_stream.writeStream.outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", "/tmp/checkpoints/aggregates")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_flash_sale_alerts(agg_stream):
    """
    Detect High-Interest / Low-Conversion windows and write alerts.
    Trigger condition: view_count > FLASH_SALE_MIN_VIEWS
                  AND purchase_count < FLASH_SALE_MAX_PURCHASES
    """
    alert_stream = (
        agg_stream.filter(
            (F.col("view_count") > FLASH_SALE_MIN_VIEWS)
            & (F.col("purchase_count") < FLASH_SALE_MAX_PURCHASES)
        )
        .select(
            F.col("product_id"),
            F.col("view_count"),
            F.col("purchase_count"),
            F.col("window_start"),
            F.col("window_end"),
            F.lit(
                "High interest detected — consider launching a Flash Sale or targeted discount."
            ).alias("alert_message"),
            F.current_timestamp().alias("created_at"),
        )
    )

    def write_alerts(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        rows = [
            tuple(row)
            for row in batch_df.select(
                "product_id",
                "view_count",
                "purchase_count",
                "window_start",
                "window_end",
                "alert_message",
                "created_at",
            ).collect()
        ]
        execute_upsert(
            """
            INSERT INTO flash_sale_alerts (
                product_id,
                view_count,
                purchase_count,
                window_start,
                window_end,
                alert_message,
                created_at
            )
            VALUES %s
            ON CONFLICT (product_id, window_start, window_end)
            DO UPDATE SET
                view_count = EXCLUDED.view_count,
                purchase_count = EXCLUDED.purchase_count,
                alert_message = EXCLUDED.alert_message,
                created_at = EXCLUDED.created_at
            """,
            rows,
        )

    return (
        alert_stream.writeStream.outputMode("update")
        .foreachBatch(write_alerts)
        .option("checkpointLocation", "/tmp/checkpoints/alerts")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_raw_events_to_postgres(stream):
    """Persist raw events for the Airflow batch layer to consume."""

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        rows = [
            tuple(row)
            for row in batch_df.select(
                "event_id",
                "user_id",
                "product_id",
                "category",
                "event_type",
                F.col("event_time").alias("event_time"),
                F.current_timestamp().alias("ingested_at"),
            ).collect()
        ]
        execute_upsert(
            """
            INSERT INTO clickstream_events (
                event_id,
                user_id,
                product_id,
                category,
                event_type,
                event_time,
                ingested_at
            )
            VALUES %s
            ON CONFLICT (event_id)
            DO NOTHING
            """,
            rows,
        )

    return (
        stream.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/checkpoints/raw_events")
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_kafka_stream(spark)
    agg_stream = aggregate_windows(raw_stream)

    raw_query = write_raw_events_to_postgres(raw_stream)
    agg_query = write_aggregates_to_postgres(agg_stream)
    alert_query = write_flash_sale_alerts(agg_stream)

    print("[INFO] Spark Structured Streaming jobs started.", flush=True)

    for query in [raw_query, agg_query, alert_query]:
        query.awaitTermination()


if __name__ == "__main__":
    main()
