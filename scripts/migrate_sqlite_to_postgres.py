import argparse
import os
import sqlite3

from database import Database


TABLES = [
    {
        "name": "listings",
        "columns": [
            "id",
            "article_no",
            "region",
            "district",
            "property_type",
            "trade_type",
            "price",
            "area",
            "floor",
            "building_name",
            "description",
            "is_urgent",
            "tags",
            "confirmed_date",
            "crawled_at",
            "crawl_session",
            "latitude",
            "longitude",
            "naver_url",
            "price_sort_value",
            "rent_sort_value",
        ],
    },
    {
        "name": "crawl_history",
        "columns": [
            "id",
            "session_id",
            "crawled_at",
            "total_count",
            "urgent_count",
            "status",
            "source",
        ],
    },
    {
        "name": "alert_rules",
        "columns": [
            "id",
            "client_id",
            "name",
            "keyword",
            "district",
            "property_type",
            "trade_type",
            "enabled",
            "created_at",
        ],
    },
    {
        "name": "alert_deliveries",
        "columns": ["id", "alert_id", "article_no", "delivered_at"],
    },
    {
        "name": "push_subscriptions",
        "columns": [
            "id",
            "client_id",
            "endpoint",
            "subscription_json",
            "created_at",
            "updated_at",
            "last_success_at",
        ],
    },
]


def quote_columns(columns):
    return ", ".join(columns)


def placeholders(count):
    return ", ".join("?" for _ in range(count))


def build_upsert_sql(table_name, columns):
    update_columns = [column for column in columns if column != "id"]
    update_sql = ", ".join(f"{column} = excluded.{column}" for column in update_columns)
    return f"""
        INSERT INTO {table_name} ({quote_columns(columns)})
        VALUES ({placeholders(len(columns))})
        ON CONFLICT (id) DO UPDATE SET
        {update_sql}
    """


def reset_sequence(conn, table_name):
    conn.execute(
        """
        SELECT setval(
            pg_get_serial_sequence(?, 'id'),
            COALESCE((SELECT MAX(id) FROM """
        + table_name
        + """), 1),
            true
        )
        """,
        (table_name,),
    )


def migrate(sqlite_path: str, database_url: str, truncate: bool):
    source = sqlite3.connect(sqlite_path)
    source.row_factory = sqlite3.Row
    destination = Database(database_url=database_url)

    if destination.driver != "postgres":
        raise RuntimeError("destination must be a Postgres DATABASE_URL")

    with destination.get_connection() as conn:
        if truncate:
            for table in [
                "alert_deliveries",
                "push_subscriptions",
                "alert_rules",
                "crawl_history",
                "listings",
            ]:
                conn.execute(f"DELETE FROM {table}")

        for table in TABLES:
            rows = source.execute(
                f"SELECT {quote_columns(table['columns'])} FROM {table['name']}"
            ).fetchall()
            if not rows:
                print(f"{table['name']}: 0 rows")
                continue

            sql = build_upsert_sql(table["name"], table["columns"])
            for row in rows:
                conn.execute(sql, tuple(row[column] for column in table["columns"]))

            reset_sequence(conn, table["name"])
            print(f"{table['name']}: {len(rows)} rows")

    source.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sqlite-path",
        default="real_estate.db",
        help="source SQLite database path",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="destination Postgres DATABASE_URL",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="clear destination tables before import",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    migrate(args.sqlite_path, args.database_url, args.truncate)


if __name__ == "__main__":
    main()
