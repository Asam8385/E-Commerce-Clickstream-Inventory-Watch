import os
from urllib.parse import urlparse

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


PIPELINE_DB_CONN = os.getenv(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres:5432/pipeline",
)


def get_connection_kwargs() -> dict:
    parsed = urlparse(PIPELINE_DB_CONN.replace("postgresql+psycopg2://", "postgresql://", 1))
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }


def generate_user_segments(**context) -> None:
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    segment_date = interval_start.date()

    connection = psycopg2.connect(**get_connection_kwargs())
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM user_segments WHERE segment_date = %s",
                (segment_date,),
            )
            cursor.execute(
                """
                INSERT INTO user_segments (
                    segment_date,
                    user_id,
                    segment,
                    total_views,
                    total_purchases
                )
                SELECT
                    %s AS segment_date,
                    user_id,
                    CASE
                        WHEN SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) > 0
                            THEN 'Buyer'
                        ELSE 'Window Shopper'
                    END AS segment,
                    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases
                FROM clickstream_events
                WHERE event_time >= %s AND event_time < %s
                GROUP BY user_id
                """,
                (segment_date, interval_start, interval_end),
            )
        connection.commit()
    finally:
        connection.close()


with DAG(
    dag_id="user_segmentation",
    description="Daily user segmentation into Buyers and Window Shoppers.",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["pipeline", "segmentation", "retail"],
) as dag:
    PythonOperator(
        task_id="generate_user_segments",
        python_callable=generate_user_segments,
    )