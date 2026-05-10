import os
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


PIPELINE_DB_CONN = os.getenv(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres:5432/pipeline",
)
REPORT_OUTPUT_DIR = Path(
    os.getenv("AIRFLOW_REPORT_OUTPUT_DIR", "/opt/airflow/reports/output")
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


def build_report_contents(report_date: str, rows: List[Tuple]) -> str:
    header = [
        "E-Commerce Daily Top Product Summary",
        f"Report Date: {report_date}",
        "",
        "Rank | Product ID | Views | Purchases | Conversion Rate",
        "-----|------------|-------|-----------|----------------",
    ]
    lines = [
        f"{rank} | {product_id} | {total_views} | {total_purchases} | {conversion_rate:.2%}"
        for rank, product_id, total_views, total_purchases, conversion_rate in rows
    ]
    if not lines:
        lines.append("No clickstream activity was recorded for this interval.")
    return "\n".join(header + lines) + "\n"


def generate_daily_product_report(**context) -> None:
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    report_date = interval_start.date()

    connection = psycopg2.connect(**get_connection_kwargs())
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM daily_product_report WHERE report_date = %s",
                (report_date,),
            )
            cursor.execute(
                """
                WITH ranked_products AS (
                    SELECT
                        ROW_NUMBER() OVER (
                            ORDER BY
                                SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) DESC,
                                product_id ASC
                        ) AS rank,
                        product_id,
                        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
                        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
                        CASE
                            WHEN SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) = 0 THEN 0
                            ELSE ROUND(
                                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)::numeric
                                / NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0),
                                4
                            )
                        END AS conversion_rate
                    FROM clickstream_events
                    WHERE event_time >= %s AND event_time < %s
                    GROUP BY product_id
                )
                INSERT INTO daily_product_report (
                    report_date,
                    rank,
                    product_id,
                    total_views,
                    total_purchases,
                    conversion_rate
                )
                SELECT
                    %s,
                    rank,
                    product_id,
                    total_views,
                    total_purchases,
                    conversion_rate
                FROM ranked_products
                WHERE rank <= 5
                ORDER BY rank
                """,
                (interval_start, interval_end, report_date),
            )
            cursor.execute(
                """
                SELECT rank, product_id, total_views, total_purchases, conversion_rate
                FROM daily_product_report
                WHERE report_date = %s
                ORDER BY rank
                """,
                (report_date,),
            )
            rows = cursor.fetchall()
        connection.commit()
    finally:
        connection.close()

    REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_OUTPUT_DIR / f"top_products_{report_date}.txt"
    report_path.write_text(
        build_report_contents(str(report_date), rows),
        encoding="utf-8",
    )


with DAG(
    dag_id="daily_product_report",
    description="Daily top-5 products report with conversion metrics.",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["pipeline", "reporting", "retail"],
) as dag:
    PythonOperator(
        task_id="generate_daily_product_report",
        python_callable=generate_daily_product_report,
    )