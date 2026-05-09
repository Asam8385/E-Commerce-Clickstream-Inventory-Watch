from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.spark_config import (
    FLASH_SALE_MAX_PURCHASES,
    FLASH_SALE_MIN_VIEWS,
    JDBC_PROPERTIES,
    JDBC_URL,
    SPARK_MASTER,
    SPARK_PACKAGES,
)


def create_session() -> SparkSession:
    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName("FlashSaleTriggerAudit")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .getOrCreate()
    )


def main() -> None:
    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")

    aggregates = (
        spark.read.jdbc(
            url=JDBC_URL,
            table="product_view_aggregates",
            properties=JDBC_PROPERTIES,
        )
    )

    flash_sale_candidates = (
        aggregates.filter(
            (F.col("view_count") > FLASH_SALE_MIN_VIEWS)
            & (F.col("purchase_count") < FLASH_SALE_MAX_PURCHASES)
        )
        .select(
            "product_id",
            "view_count",
            "purchase_count",
            "window_start",
            "window_end",
            F.lit(
                "High interest detected — consider launching a Flash Sale or targeted discount."
            ).alias("alert_message"),
            F.current_timestamp().alias("created_at"),
        )
    )

    if flash_sale_candidates.limit(1).count() == 0:
        print("[INFO] No flash-sale candidates found.", flush=True)
    else:
        flash_sale_candidates.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
