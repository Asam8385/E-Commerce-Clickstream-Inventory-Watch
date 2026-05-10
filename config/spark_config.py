import os

# ---------------------------------------------------------------------------
# Spark job configuration
# ---------------------------------------------------------------------------

SPARK_MASTER: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
APP_NAME: str = "EcommerceClickstreamProcessor"

# Kafka source options for Spark Structured Streaming
KAFKA_SOURCE_OPTIONS: dict = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "subscribe": os.getenv("KAFKA_TOPIC", "clickstream-events"),
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
}

# Sliding-window parameters (ISO-8601 duration strings)
WINDOW_DURATION: str = "10 minutes"
SLIDE_DURATION: str = "1 minute"
WATERMARK_DELAY: str = "2 minutes"

# Flash-sale trigger thresholds
FLASH_SALE_MIN_VIEWS: int = 100
FLASH_SALE_MAX_PURCHASES: int = 5

# PostgreSQL sink
PG_HOST: str = os.getenv("PG_HOST", "postgres")
PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
PG_DB: str = os.getenv("PG_DB", "pipeline")
PG_USER: str = os.getenv("PG_USER", "pipeline")
PG_PASSWORD: str = os.getenv("PG_PASSWORD", "pipeline")

JDBC_URL: str = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPERTIES: dict = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Spark packages (passed via --packages or SparkConf)
SPARK_PACKAGES: str = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.postgresql:postgresql:42.6.0"
)
