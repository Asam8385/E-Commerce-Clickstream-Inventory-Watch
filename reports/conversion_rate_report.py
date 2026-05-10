import csv
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import psycopg2


BASE_DIR = Path(__file__).resolve().parent
PIPELINE_DB_CONN = os.getenv(
    "PIPELINE_DB_CONN",
    "postgresql://pipeline:pipeline@localhost:5432/pipeline",
)
REPORT_DATE = os.getenv("REPORT_DATE")
OUTPUT_DIR = Path(os.getenv("REPORT_OUTPUT_DIR", str(BASE_DIR / "output")))


def get_connection_kwargs() -> dict:
    normalized = PIPELINE_DB_CONN.replace("postgresql+psycopg2://", "postgresql://", 1)
    parsed = urlparse(normalized)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }


def resolve_report_date() -> date:
    if REPORT_DATE:
        return date.fromisoformat(REPORT_DATE)
    return datetime.now(timezone.utc).date()


def fetch_conversion_rows(report_date: date) -> list[tuple]:
    connection = psycopg2.connect(**get_connection_kwargs())
    start_ts = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc)
    end_ts = start_ts + timedelta(days=1)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(category, 'Unknown') AS category,
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
                GROUP BY COALESCE(category, 'Unknown'), product_id
                HAVING SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) > 0
                ORDER BY category ASC, conversion_rate DESC, total_views DESC, product_id ASC
                """,
                (start_ts, end_ts),
            )
            return cursor.fetchall()
    finally:
        connection.close()


def write_csv_report(report_date: date, rows: list[tuple]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"conversion_rates_{report_date}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "category",
                "product_id",
                "total_views",
                "total_purchases",
                "conversion_rate",
            ]
        )
        for category, product_id, total_views, total_purchases, conversion_rate in rows:
            writer.writerow(
                [
                    category,
                    product_id,
                    total_views,
                    total_purchases,
                    f"{float(conversion_rate):.4f}",
                ]
            )
    return output_path


def write_category_summary(report_date: date, rows: list[tuple]) -> Path:
    category_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {"views": 0, "purchases": 0}
    )
    for category, _, total_views, total_purchases, _ in rows:
        category_totals[category]["views"] += total_views
        category_totals[category]["purchases"] += total_purchases

    output_path = OUTPUT_DIR / f"category_conversion_summary_{report_date}.txt"
    lines = [
        "Conversion Rate Summary by Product Category",
        f"Report Date: {report_date}",
        "",
        "Category | Views | Purchases | Conversion Rate",
        "---------|-------|-----------|----------------",
    ]
    for category in sorted(category_totals):
        views = int(category_totals[category]["views"])
        purchases = int(category_totals[category]["purchases"])
        conversion_rate = 0 if views == 0 else purchases / views
        lines.append(
            f"{category} | {views} | {purchases} | {conversion_rate:.2%}"
        )
    if not category_totals:
        lines.append("No qualifying view activity was recorded for the selected reporting date.")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def main() -> None:
    report_date = resolve_report_date()
    rows = fetch_conversion_rows(report_date)
    csv_path = write_csv_report(report_date, rows)
    summary_path = write_category_summary(report_date, rows)

    print(f"[INFO] Wrote conversion CSV report: {csv_path}")
    print(f"[INFO] Wrote category summary report: {summary_path}")


if __name__ == "__main__":
    main()