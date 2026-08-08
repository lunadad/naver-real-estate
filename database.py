import os
import json
import logging
import sqlite3
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import psycopg
except ImportError:  # pragma: no cover - optional for sqlite-only use
    psycopg = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - optional for sqlite-only use
    ConnectionPool = None

logger = logging.getLogger(__name__)

# 물리적으로 다른 주택이 같은 이름으로 합쳐지는 것을 막기 위해,
# 고유 건물명이 아닌 매물 유형명은 건물별 스냅샷 집계에서 제외한다.
GENERIC_BUILDING_NAMES = frozenset(
    {
        "아파트",
        "오피스텔",
        "빌라/연립",
        "빌라",
        "연립",
        "연립주택",
        "다세대",
        "다세대주택",
        "단독/다가구",
        "단독주택",
        "다가구",
        "다가구주택",
        "주택",
        "상가/업무",
        "상가",
        "상가주택",
        "근린상가",
        "원룸",
    }
)


def ensure_postgres_sslmode(database_url: str) -> str:
    if not database_url.startswith(("postgresql://", "postgres://")):
        return database_url

    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class CompatRow(dict):
    def __init__(self, columns: Sequence[str], values: Sequence[object]):
        super().__init__(zip(columns, values))
        self._values = tuple(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


class CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def rowcount(self):
        return getattr(self.cursor, "rowcount", 0)

    @property
    def lastrowid(self):
        return getattr(self.cursor, "lastrowid", None)

    def _convert_row(self, row):
        if row is None:
            return None
        if isinstance(row, CompatRow):
            return row
        if isinstance(row, sqlite3.Row):
            columns = row.keys()
            values = tuple(row)
        else:
            columns = [desc[0] for desc in (self.cursor.description or [])]
            values = tuple(row)
        return CompatRow(columns, values)

    def fetchone(self):
        return self._convert_row(self.cursor.fetchone())

    def fetchall(self):
        return [self._convert_row(row) for row in self.cursor.fetchall()]


class ConnectionWrapper:
    def __init__(self, driver: str, conn, release=None):
        self.driver = driver
        self.conn = conn
        self.release = release

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        had_error = exc_type is not None
        try:
            if exc_type:
                try:
                    self.conn.rollback()
                except Exception:
                    logger.warning("Failed to roll back database transaction", exc_info=True)
            else:
                try:
                    self.conn.commit()
                except Exception:
                    had_error = True
                    raise
        finally:
            try:
                if self.release:
                    self.release(exc_type, exc, tb)
                else:
                    self.conn.close()
            except Exception:
                if had_error:
                    logger.warning("Failed to release database connection", exc_info=True)
                else:
                    raise

    def _convert_sql(self, sql: str) -> str:
        if self.driver == "postgres":
            return sql.replace("%", "%%").replace("?", "%s")
        return sql

    def execute(self, sql: str, params: Optional[Sequence[object]] = None):
        params = tuple(params or [])
        if self.driver == "postgres":
            cursor = self.conn.cursor()
            cursor.execute(self._convert_sql(sql), params)
            return CursorWrapper(cursor)
        return CursorWrapper(self.conn.execute(sql, params))

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[object]]):
        rows = [tuple(params) for params in seq_of_params]
        if not rows:
            return None

        if self.driver == "postgres":
            cursor = self.conn.cursor()
            cursor.executemany(self._convert_sql(sql), rows)
            return CursorWrapper(cursor)

        cursor = self.conn.cursor()
        cursor.executemany(sql, rows)
        return CursorWrapper(cursor)

    def executescript(self, script: str):
        if self.driver == "sqlite":
            self.conn.executescript(script)
            return

        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                self.execute(statement)


class Database:
    def __init__(
        self,
        db_path="real_estate.db",
        database_url: Optional[str] = None,
        skip_price_backfill: bool = False,
    ):
        self.db_path = db_path
        self.database_url = ensure_postgres_sslmode((database_url or "").strip())
        self.driver = "postgres" if self.database_url else "sqlite"
        self.skip_price_backfill = skip_price_backfill
        self.connect_timeout = int((os.getenv("PGCONNECT_TIMEOUT") or "10").strip())
        self.pool = None
        self._open_pool()
        self.init_db()

    def _open_pool(self):
        if self.driver != "postgres" or ConnectionPool is None:
            return

        pool_kwargs = {
            "kwargs": {"connect_timeout": self.connect_timeout},
            "min_size": int((os.getenv("DB_POOL_MIN_SIZE") or "1").strip()),
            "max_size": int((os.getenv("DB_POOL_MAX_SIZE") or "5").strip()),
            "timeout": float((os.getenv("DB_POOL_TIMEOUT") or "10").strip()),
            "open": True,
        }
        if hasattr(ConnectionPool, "check_connection"):
            pool_kwargs["check"] = ConnectionPool.check_connection

        self.pool = ConnectionPool(self.database_url, **pool_kwargs)

    def get_connection(self):
        if self.driver == "postgres":
            if psycopg is None:
                raise RuntimeError(
                    "Postgres support requires psycopg. Install requirements first."
                )
            if self.pool is not None:
                pool_conn = self.pool.connection()
                conn = pool_conn.__enter__()
                return ConnectionWrapper(
                    "postgres",
                    conn,
                    release=lambda exc_type, exc, tb: pool_conn.__exit__(
                        exc_type, exc, tb
                    ),
                )

            conn = psycopg.connect(self.database_url, connect_timeout=self.connect_timeout)
            return ConnectionWrapper("postgres", conn)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return ConnectionWrapper("sqlite", conn)

    def close(self):
        if self.pool is not None:
            self.pool.close()
            self.pool = None

    def reconnect(self):
        self.close()
        self._open_pool()

    def is_transient_connection_error(self, exc: Exception) -> bool:
        if self.driver != "postgres":
            return False

        if psycopg is not None and isinstance(
            exc, (psycopg.OperationalError, psycopg.InterfaceError)
        ):
            return True

        transient_names = {
            "AdminShutdown",
            "ConnectionDoesNotExist",
            "ConnectionFailure",
            "ConnectionException",
        }
        if exc.__class__.__name__ in transient_names:
            return True

        message = str(exc).lower()
        return "connection is lost" in message or "terminating connection" in message

    def init_db(self):
        with self.get_connection() as conn:
            if self.driver == "postgres":
                self._init_postgres(conn)
            else:
                self._init_sqlite(conn)

            cols = self._get_table_columns(conn, "listings")
            if "naver_url" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN naver_url TEXT")
            if "price_sort_value" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN price_sort_value BIGINT")
            if "rent_sort_value" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN rent_sort_value BIGINT")

            alert_cols = self._get_table_columns(conn, "alert_rules")
            if "building_name" not in alert_cols:
                conn.execute("ALTER TABLE alert_rules ADD COLUMN building_name TEXT")
            if "min_daily_change" not in alert_cols:
                conn.execute(
                    "ALTER TABLE alert_rules ADD COLUMN min_daily_change INTEGER DEFAULT 0"
                )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_sort ON listings(price_sort_value)"
            )
            if not self.skip_price_backfill:
                self._backfill_price_sort_values(conn)
            latest_visible_session = self._get_latest_visible_session_id(conn)
            if latest_visible_session:
                existing_stats = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM crawl_region_stats WHERE session_id = ?",
                    (latest_visible_session,),
                ).fetchone()["cnt"]
                if not existing_stats:
                    self.rebuild_crawl_region_stats_from_listings(latest_visible_session, conn)

    def _init_sqlite(self, conn: ConnectionWrapper):
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_no TEXT UNIQUE,
                region TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                price TEXT,
                area TEXT,
                floor TEXT,
                building_name TEXT,
                description TEXT,
                is_urgent INTEGER DEFAULT 0,
                tags TEXT,
                confirmed_date TEXT,
                crawled_at TEXT,
                crawl_session TEXT,
                latitude REAL,
                longitude REAL,
                naver_url TEXT,
                price_sort_value INTEGER,
                rent_sort_value INTEGER
            );

            CREATE TABLE IF NOT EXISTS _migrations (
                id INTEGER PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS crawl_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                crawled_at TEXT,
                total_count INTEGER,
                urgent_count INTEGER,
                status TEXT,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS crawl_region_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                UNIQUE(session_id, region, district)
            );

            CREATE TABLE IF NOT EXISTS crawl_building_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                building_name TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                UNIQUE(session_id, district, building_name)
            );

            CREATE TABLE IF NOT EXISTS alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                building_name TEXT,
                min_daily_change INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS alert_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                article_no TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE(alert_id, article_no)
            );

            CREATE TABLE IF NOT EXISTS building_alert_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE(alert_id, session_id)
            );

            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                endpoint TEXT NOT NULL UNIQUE,
                subscription_json TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT,
                last_success_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_region ON listings(region);
            CREATE INDEX IF NOT EXISTS idx_district ON listings(district);
            CREATE INDEX IF NOT EXISTS idx_property_type ON listings(property_type);
            CREATE INDEX IF NOT EXISTS idx_is_urgent ON listings(is_urgent);
            CREATE INDEX IF NOT EXISTS idx_crawled_at ON listings(crawled_at);
            CREATE INDEX IF NOT EXISTS idx_session ON listings(crawl_session);
            CREATE INDEX IF NOT EXISTS idx_crawl_region_stats_session ON crawl_region_stats(session_id);
            CREATE INDEX IF NOT EXISTS idx_building_stats_lookup ON crawl_building_stats(district, building_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_building_stats_created_at ON crawl_building_stats(created_at);
            CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id);
            CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id);
            CREATE INDEX IF NOT EXISTS idx_building_alert_deliveries_alert_id ON building_alert_deliveries(alert_id);
            CREATE INDEX IF NOT EXISTS idx_push_subscriptions_client_id ON push_subscriptions(client_id);
            """
        )

    def _init_postgres(self, conn: ConnectionWrapper):
        statements = [
            """
            CREATE TABLE IF NOT EXISTS listings (
                id BIGSERIAL PRIMARY KEY,
                article_no TEXT UNIQUE,
                region TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                price TEXT,
                area TEXT,
                floor TEXT,
                building_name TEXT,
                description TEXT,
                is_urgent INTEGER DEFAULT 0,
                tags TEXT,
                confirmed_date TEXT,
                crawled_at TIMESTAMP,
                crawl_session TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                naver_url TEXT,
                price_sort_value BIGINT,
                rent_sort_value BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS _migrations (
                id BIGSERIAL PRIMARY KEY
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crawl_history (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT,
                crawled_at TIMESTAMP,
                total_count INTEGER,
                urgent_count INTEGER,
                status TEXT,
                source TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crawl_region_stats (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP,
                UNIQUE(session_id, region, district)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crawl_building_stats (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                building_name TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP,
                UNIQUE(session_id, district, building_name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id BIGSERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                building_name TEXT,
                min_daily_change INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alert_deliveries (
                id BIGSERIAL PRIMARY KEY,
                alert_id BIGINT NOT NULL,
                article_no TEXT NOT NULL,
                delivered_at TIMESTAMP,
                UNIQUE(alert_id, article_no)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS building_alert_deliveries (
                id BIGSERIAL PRIMARY KEY,
                alert_id BIGINT NOT NULL,
                session_id TEXT NOT NULL,
                delivered_at TIMESTAMP,
                UNIQUE(alert_id, session_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id BIGSERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                endpoint TEXT NOT NULL UNIQUE,
                subscription_json TEXT NOT NULL,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                last_success_at TIMESTAMP
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_region ON listings(region)",
            "CREATE INDEX IF NOT EXISTS idx_district ON listings(district)",
            "CREATE INDEX IF NOT EXISTS idx_property_type ON listings(property_type)",
            "CREATE INDEX IF NOT EXISTS idx_is_urgent ON listings(is_urgent)",
            "CREATE INDEX IF NOT EXISTS idx_crawled_at ON listings(crawled_at)",
            "CREATE INDEX IF NOT EXISTS idx_session ON listings(crawl_session)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_region_stats_session ON crawl_region_stats(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_building_stats_lookup ON crawl_building_stats(district, building_name, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_building_stats_created_at ON crawl_building_stats(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id)",
            "CREATE INDEX IF NOT EXISTS idx_building_alert_deliveries_alert_id ON building_alert_deliveries(alert_id)",
            "CREATE INDEX IF NOT EXISTS idx_push_subscriptions_client_id ON push_subscriptions(client_id)",
        ]
        for statement in statements:
            conn.execute(statement)

    def _get_table_columns(self, conn: ConnectionWrapper, table_name: str):
        if self.driver == "postgres":
            rows = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                """,
                (table_name,),
            ).fetchall()
            return {row["column_name"] for row in rows}

        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def _parse_low_unit_manwon(self, text: str) -> Optional[int]:
        normalized = re.sub(r"\s+", "", text or "")
        normalized = normalized.replace(",", "").replace("만원", "만")
        normalized = normalized.replace("만", "").replace("원", "")
        if not normalized:
            return None
        if normalized.isdigit():
            return int(normalized)

        unit_map = {"천": 1000, "백": 100, "십": 10}
        total = 0
        for number, unit in re.findall(r"(\d+)(천|백|십)", normalized):
            total += int(number) * unit_map[unit]

        remainder = re.sub(r"(\d+)(천|백|십)", "", normalized)
        if remainder:
            if remainder.isdigit():
                total += int(remainder)
            else:
                digits = re.findall(r"\d+", remainder)
                if digits:
                    total += int("".join(digits))

        return total if total > 0 else None

    def _parse_money_to_manwon(self, raw: Optional[str]) -> Optional[int]:
        text = re.sub(r"\s+", "", str(raw or ""))
        if not text or not re.search(r"\d", text):
            return None

        if "억" in text:
            eok_part, rest = text.split("억", 1)
            eok_digits = re.sub(r"[^\d]", "", eok_part)
            total = (int(eok_digits) if eok_digits else 0) * 10000
            low_units = self._parse_low_unit_manwon(rest)
            return total + (low_units or 0)

        return self._parse_low_unit_manwon(text)

    def _parse_price_sort_values(
        self, price: Optional[str], trade_type: Optional[str]
    ) -> Tuple[Optional[int], Optional[int]]:
        text = str(price or "").strip()
        if not text:
            return None, None

        if "/" in text:
            deposit_raw, monthly_raw = text.split("/", 1)
            return (
                self._parse_money_to_manwon(deposit_raw),
                self._parse_money_to_manwon(monthly_raw),
            )

        price_value = self._parse_money_to_manwon(text)
        if trade_type == "월세":
            return price_value, 0
        return price_value, None

    def _backfill_price_sort_values(self, conn: ConnectionWrapper):
        rows = conn.execute(
            """
            SELECT id, price, trade_type
            FROM listings
            WHERE price_sort_value IS NULL OR (trade_type = '월세' AND rent_sort_value IS NULL)
            """
        ).fetchall()

        for row in rows:
            price_value, rent_value = self._parse_price_sort_values(
                row["price"], row["trade_type"]
            )
            conn.execute(
                """
                UPDATE listings
                SET price_sort_value = ?, rent_sort_value = ?
                WHERE id = ?
                """,
                (price_value, rent_value, row["id"]),
            )

    def _get_latest_session_id(
        self,
        conn: ConnectionWrapper,
        *,
        success_only: bool = False,
        exclude_demo: bool = False,
    ) -> Optional[str]:
        joins = ""
        conditions = ["l.crawl_session IS NOT NULL"]

        if success_only or exclude_demo:
            joins = "LEFT JOIN crawl_history h ON h.session_id = l.crawl_session"
        if success_only:
            conditions.append("h.status = 'success'")
        if exclude_demo:
            conditions.append("COALESCE(h.source, 'naver') <> 'demo'")

        row = conn.execute(
            f"""
            SELECT l.crawl_session, MAX(l.crawled_at) AS last_seen
            FROM listings l
            {joins}
            WHERE {" AND ".join(conditions)}
            GROUP BY l.crawl_session
            ORDER BY last_seen DESC
            LIMIT 1
            """
        ).fetchone()
        return row["crawl_session"] if row else None

    def _get_latest_visible_session_id(self, conn: ConnectionWrapper) -> Optional[str]:
        return self._get_latest_session_id(
            conn,
            success_only=True,
            exclude_demo=True,
        ) or self._get_latest_session_id(conn)

    def _normalize_alert_value(self, value: Optional[str]) -> str:
        return str(value or "").strip()

    def _build_alert_name(
        self,
        keyword: str,
        district: str,
        property_type: str,
        trade_type: str,
        building_name: str = "",
        min_daily_change: int = 0,
    ) -> str:
        if building_name and min_daily_change > 0:
            return f"{building_name} 하루 ±{min_daily_change}건"
        parts = []
        if keyword:
            parts.append(keyword)
        if district:
            parts.append(district)
        if property_type:
            parts.append(property_type)
        if trade_type:
            parts.append(trade_type)
        return " · ".join(parts) if parts else "전체 급매 알림"

    def _build_region_stats_rows(self, session_id: str, listings: List[Dict], created_at: str):
        grouped = {}
        for listing in listings:
            region = str(listing.get("region") or "").strip()
            district = str(listing.get("district") or "").strip()
            if not region or not district:
                continue
            key = (region, district)
            entry = grouped.setdefault(
                key,
                {
                    "session_id": session_id,
                    "region": region,
                    "district": district,
                    "total_count": 0,
                    "price_down_count": 0,
                    "created_at": created_at,
                },
            )
            entry["total_count"] += 1
            tags = listing.get("tags") or []
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except Exception:
                    tags = [tags]
            if "가격인하" in tags:
                entry["price_down_count"] += 1
        return list(grouped.values())

    def _build_building_stats_rows(self, session_id: str, listings: List[Dict], created_at: str):
        grouped = {}
        for listing in listings:
            region = str(listing.get("region") or "").strip()
            district = str(listing.get("district") or "").strip()
            building_name = str(listing.get("building_name") or "").strip()
            if not district or not building_name:
                continue
            if building_name in GENERIC_BUILDING_NAMES:
                continue
            key = (district, building_name)
            entry = grouped.setdefault(
                key,
                {
                    "session_id": session_id,
                    "region": region,
                    "district": district,
                    "building_name": building_name,
                    "total_count": 0,
                    "price_down_count": 0,
                    "created_at": created_at,
                },
            )
            entry["total_count"] += 1
            tags = listing.get("tags") or []
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except Exception:
                    tags = [tags]
            if "가격인하" in tags:
                entry["price_down_count"] += 1
        return [row for row in grouped.values() if row["total_count"] >= 2]

    def replace_crawl_region_stats(
        self,
        session_id: str,
        rows: List[Dict],
        conn: Optional[ConnectionWrapper] = None,
    ):
        if not session_id:
            return

        insert_sql = """
            INSERT INTO crawl_region_stats
            (session_id, region, district, total_count, price_down_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, region, district) DO UPDATE SET
                total_count = excluded.total_count,
                price_down_count = excluded.price_down_count,
                created_at = excluded.created_at
        """

        owns_connection = conn is None
        conn = conn or self.get_connection()
        if owns_connection:
            conn.__enter__()
        try:
            conn.execute(
                "DELETE FROM crawl_region_stats WHERE session_id = ?",
                (session_id,),
            )
            payload = [
                (
                    row["session_id"],
                    row["region"],
                    row["district"],
                    row["total_count"],
                    row.get("price_down_count", 0),
                    row.get("created_at", datetime.now().isoformat()),
                )
                for row in rows
            ]
            if payload:
                conn.executemany(insert_sql, payload)
        finally:
            if owns_connection:
                conn.__exit__(None, None, None)

    def rebuild_crawl_region_stats_from_listings(
        self, session_id: str, conn: Optional[ConnectionWrapper] = None
    ) -> bool:
        if not session_id:
            return False

        owns_connection = conn is None
        conn = conn or self.get_connection()
        if owns_connection:
            conn.__enter__()
        try:
            history = conn.execute(
                """
                SELECT total_count
                FROM crawl_history
                WHERE session_id = ?
                ORDER BY crawled_at DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if not history:
                return False

            listing_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM listings WHERE crawl_session = ?",
                (session_id,),
            ).fetchone()["cnt"]
            if int(history["total_count"] or 0) != int(listing_count or 0):
                return False

            rows = conn.execute(
                """
                SELECT region,
                       district,
                       COUNT(*) AS total_count,
                       SUM(CASE WHEN tags LIKE '%가격인하%' THEN 1 ELSE 0 END) AS price_down_count
                FROM listings
                WHERE crawl_session = ?
                GROUP BY region, district
                """,
                (session_id,),
            ).fetchall()

            payload = [
                {
                    "session_id": session_id,
                    "region": row["region"],
                    "district": row["district"],
                    "total_count": row["total_count"],
                    "price_down_count": row["price_down_count"] or 0,
                    "created_at": datetime.now().isoformat(),
                }
                for row in rows
            ]
            self.replace_crawl_region_stats(session_id, payload, conn)
            return True
        finally:
            if owns_connection:
                conn.__exit__(None, None, None)

    def create_alert_rule(
        self,
        client_id: str,
        keyword: str = "",
        district: str = "",
        property_type: str = "",
        trade_type: str = "",
        building_name: str = "",
        min_daily_change: int = 0,
        name: str = "",
    ):
        client_id = self._normalize_alert_value(client_id)
        keyword = self._normalize_alert_value(keyword)
        district = self._normalize_alert_value(district)
        property_type = self._normalize_alert_value(property_type)
        trade_type = self._normalize_alert_value(trade_type)
        building_name = self._normalize_alert_value(building_name)
        min_daily_change = max(0, int(min_daily_change or 0))
        name = self._normalize_alert_value(name) or self._build_alert_name(
            keyword,
            district,
            property_type,
            trade_type,
            building_name,
            min_daily_change,
        )

        with self.get_connection() as conn:
            if self.driver == "postgres":
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type,
                     building_name, min_daily_change, enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    RETURNING id
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
                        building_name,
                        min_daily_change,
                        datetime.now().isoformat(),
                    ),
                )
                rule_id = cursor.fetchone()["id"]
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type,
                     building_name, min_daily_change, enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
                        building_name,
                        min_daily_change,
                        datetime.now().isoformat(),
                    ),
                )
                rule_id = cursor.lastrowid

        return self.get_alert_rule(client_id, rule_id)

    def get_alert_rule(self, client_id: str, alert_id: int):
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM alert_rules
                WHERE client_id = ? AND id = ?
                """,
                (client_id, alert_id),
            ).fetchone()
        return dict(row) if row else None

    def get_alert_rules(self, client_id: str):
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM alert_rules
                WHERE client_id = ?
                ORDER BY created_at DESC
                """,
                (client_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_alert_rule(self, client_id: str, alert_id: int) -> bool:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM alert_rules WHERE client_id = ? AND id = ?",
                (client_id, alert_id),
            ).fetchone()
            if not row:
                return False

            conn.execute("DELETE FROM alert_deliveries WHERE alert_id = ?", (alert_id,))
            conn.execute(
                "DELETE FROM building_alert_deliveries WHERE alert_id = ?", (alert_id,)
            )
            conn.execute(
                "DELETE FROM alert_rules WHERE client_id = ? AND id = ?",
                (client_id, alert_id),
            )
            return True

    def _collect_alert_matches(self, conn: ConnectionWrapper, client_id: str, limit: int = 10):
        latest_session = self._get_latest_visible_session_id(conn)
        if not latest_session:
            return []

        rules = conn.execute(
            """
            SELECT *
            FROM alert_rules
            WHERE client_id = ? AND enabled = 1
              AND COALESCE(min_daily_change, 0) = 0
            ORDER BY created_at DESC
            """,
            (client_id,),
        ).fetchall()

        matches_by_article = {}
        for rule in rules:
            conditions = [
                "crawl_session = ?",
                "crawled_at >= ?",
            ]
            params = [latest_session, rule["created_at"]]

            if rule["keyword"]:
                conditions.append(
                    "(region LIKE ? OR district LIKE ? OR building_name LIKE ? OR description LIKE ?)"
                )
                params.extend([f"%{rule['keyword']}%"] * 4)
            if rule["district"]:
                conditions.append("district = ?")
                params.append(rule["district"])
            if rule["property_type"]:
                conditions.append("property_type = ?")
                params.append(rule["property_type"])
            if rule["trade_type"]:
                conditions.append("trade_type = ?")
                params.append(rule["trade_type"])

            rows = conn.execute(
                f"""
                SELECT *
                FROM listings
                WHERE {" AND ".join(conditions)}
                  AND NOT EXISTS (
                    SELECT 1
                    FROM alert_deliveries d
                    WHERE d.alert_id = ? AND d.article_no = listings.article_no
                  )
                ORDER BY confirmed_date DESC, crawled_at DESC
                LIMIT ?
                """,
                params + [rule["id"], limit],
            ).fetchall()

            for row in rows:
                article_no = row["article_no"]
                if article_no not in matches_by_article:
                    entry = dict(row)
                    entry["alert_names"] = [rule["name"]]
                    entry["_delivery_refs"] = [(rule["id"], article_no)]
                    matches_by_article[article_no] = entry
                else:
                    matches_by_article[article_no]["alert_names"].append(rule["name"])
                    matches_by_article[article_no]["_delivery_refs"].append(
                        (rule["id"], article_no)
                    )

        matches = sorted(
            matches_by_article.values(),
            key=lambda item: (
                item.get("confirmed_date") or "",
                item.get("crawled_at") or "",
            ),
            reverse=True,
        )
        return matches[:limit]

    @staticmethod
    def _alert_datetime(value):
        if isinstance(value, datetime):
            parsed = value
        else:
            try:
                parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
            except ValueError:
                return None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def _collect_building_change_alert_matches(
        self, conn: ConnectionWrapper, client_id: str
    ):
        rules = conn.execute(
            """
            SELECT *
            FROM alert_rules
            WHERE client_id = ? AND enabled = 1
              AND COALESCE(min_daily_change, 0) > 0
            ORDER BY created_at DESC
            """,
            (client_id,),
        ).fetchall()
        if not rules:
            return []

        changes = self._get_latest_building_changes(
            conn,
            [(rule["district"], rule["building_name"]) for rule in rules],
        )
        placeholders = ", ".join(["?"] * len(rules))
        delivered_rows = conn.execute(
            f"""
            SELECT alert_id, session_id
            FROM building_alert_deliveries
            WHERE alert_id IN ({placeholders})
            """,
            [rule["id"] for rule in rules],
        ).fetchall()
        delivered = {(row["alert_id"], row["session_id"]) for row in delivered_rows}

        matches_by_event = {}
        for rule in rules:
            key = (rule["district"], rule["building_name"])
            change = changes.get(key)
            if not change or abs(change["total_diff"]) < int(rule["min_daily_change"]):
                continue

            current_at = self._alert_datetime(change["current_crawled_at"])
            previous_at = self._alert_datetime(change["previous_crawled_at"])
            created_at = self._alert_datetime(rule["created_at"])
            if not current_at or not previous_at or (current_at.date() - previous_at.date()).days != 1:
                continue
            if created_at and current_at < created_at:
                continue

            session_id = change["current_session_id"]
            if (rule["id"], session_id) in delivered:
                continue

            event_key = (session_id, rule["district"], rule["building_name"])
            if event_key not in matches_by_event:
                total_diff = change["total_diff"]
                matches_by_event[event_key] = {
                    "event_type": "building_daily_change",
                    "article_no": f"building-change:{session_id}:{rule['district']}:{rule['building_name']}",
                    "region": "",
                    "district": rule["district"],
                    "building_name": rule["building_name"],
                    "previous_count": change["previous_total"],
                    "current_count": change["current_total"],
                    "change_count": total_diff,
                    "current_session_id": session_id,
                    "crawled_at": change["current_crawled_at"],
                    "naver_url": "/",
                    "alert_names": [rule["name"]],
                    "_building_delivery_refs": [(rule["id"], session_id)],
                }
            else:
                matches_by_event[event_key]["alert_names"].append(rule["name"])
                matches_by_event[event_key]["_building_delivery_refs"].append(
                    (rule["id"], session_id)
                )
        return list(matches_by_event.values())

    def _mark_delivery_refs(self, conn: ConnectionWrapper, delivery_refs):
        delivered_at = datetime.now().isoformat()
        for alert_id, article_no in delivery_refs:
            conn.execute(
                """
                INSERT INTO alert_deliveries (alert_id, article_no, delivered_at)
                VALUES (?, ?, ?)
                ON CONFLICT(alert_id, article_no) DO NOTHING
                """,
                (alert_id, article_no, delivered_at),
            )

    def _sanitize_alert_match(self, match: Dict):
        item = dict(match)
        item.pop("_delivery_refs", None)
        item.pop("_building_delivery_refs", None)
        return item

    def get_pending_alert_matches(self, client_id: str, limit: int = 10):
        with self.get_connection() as conn:
            matches = self._collect_alert_matches(conn, client_id, limit)
            matches.extend(self._collect_building_change_alert_matches(conn, client_id))
            matches.sort(key=lambda item: str(item.get("crawled_at") or ""), reverse=True)
            return matches[:limit]

    def mark_alert_matches_delivered(self, matches: List[Dict]):
        delivery_refs = []
        building_delivery_refs = []
        for match in matches:
            delivery_refs.extend(match.get("_delivery_refs", []))
            building_delivery_refs.extend(match.get("_building_delivery_refs", []))

        if not delivery_refs and not building_delivery_refs:
            return

        with self.get_connection() as conn:
            self._mark_delivery_refs(conn, delivery_refs)
            delivered_at = datetime.now().isoformat()
            for alert_id, session_id in building_delivery_refs:
                conn.execute(
                    """
                    INSERT INTO building_alert_deliveries (alert_id, session_id, delivered_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(alert_id, session_id) DO NOTHING
                    """,
                    (alert_id, session_id, delivered_at),
                )

    def get_new_alert_matches(self, client_id: str, limit: int = 10):
        matches = self.get_pending_alert_matches(client_id, limit)
        self.mark_alert_matches_delivered(matches)
        return [self._sanitize_alert_match(match) for match in matches]

    def save_push_subscription(self, client_id: str, subscription: Dict):
        client_id = self._normalize_alert_value(client_id)
        endpoint = self._normalize_alert_value((subscription or {}).get("endpoint"))
        keys = (subscription or {}).get("keys") or {}
        auth = self._normalize_alert_value(keys.get("auth"))
        p256dh = self._normalize_alert_value(keys.get("p256dh"))

        if not client_id or not endpoint or not auth or not p256dh:
            raise ValueError("invalid push subscription")

        now = datetime.now().isoformat()
        payload = json.dumps(subscription, ensure_ascii=False)

        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO push_subscriptions
                (client_id, endpoint, subscription_json, created_at, updated_at, last_success_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(endpoint) DO UPDATE SET
                    client_id = excluded.client_id,
                    subscription_json = excluded.subscription_json,
                    updated_at = excluded.updated_at
                """,
                (client_id, endpoint, payload, now, now),
            )

    def delete_push_subscription(self, client_id: str, endpoint: str = ""):
        client_id = self._normalize_alert_value(client_id)
        endpoint = self._normalize_alert_value(endpoint)
        if not client_id:
            return 0

        with self.get_connection() as conn:
            if endpoint:
                cursor = conn.execute(
                    "DELETE FROM push_subscriptions WHERE client_id = ? AND endpoint = ?",
                    (client_id, endpoint),
                )
            else:
                cursor = conn.execute(
                    "DELETE FROM push_subscriptions WHERE client_id = ?",
                    (client_id,),
                )
            return cursor.rowcount

    def delete_push_subscription_by_endpoint(self, endpoint: str):
        endpoint = self._normalize_alert_value(endpoint)
        if not endpoint:
            return 0

        with self.get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM push_subscriptions WHERE endpoint = ?",
                (endpoint,),
            )
            return cursor.rowcount

    def get_push_subscriptions(self, client_id: Optional[str] = None):
        with self.get_connection() as conn:
            if client_id:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM push_subscriptions
                    WHERE client_id = ?
                    ORDER BY updated_at DESC, created_at DESC
                    """,
                    (client_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM push_subscriptions
                    ORDER BY updated_at DESC, created_at DESC
                    """
                ).fetchall()

        result = []
        for row in rows:
            item = dict(row)
            try:
                item["subscription"] = json.loads(item.pop("subscription_json"))
            except Exception:
                item["subscription"] = None
                item.pop("subscription_json", None)
            result.append(item)
        return result

    def get_push_client_ids(self):
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT p.client_id
                FROM push_subscriptions p
                INNER JOIN alert_rules a ON a.client_id = p.client_id
                WHERE a.enabled = 1
                ORDER BY p.client_id
                """
            ).fetchall()
        return [row["client_id"] for row in rows]

    def touch_push_subscription_success(self, endpoint: str):
        endpoint = self._normalize_alert_value(endpoint)
        if not endpoint:
            return

        with self.get_connection() as conn:
            now = datetime.now().isoformat()
            conn.execute(
                """
                UPDATE push_subscriptions
                SET last_success_at = ?, updated_at = ?
                WHERE endpoint = ?
                """,
                (now, now, endpoint),
            )

    def insert_listings(self, listings: List[Dict], session_id: str):
        insert_sql = """
            INSERT INTO listings
            (article_no, region, district, property_type, trade_type, price,
             area, floor, building_name, description, is_urgent, tags,
             confirmed_date, crawled_at, crawl_session, latitude, longitude, naver_url,
             price_sort_value, rent_sort_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(article_no) DO UPDATE SET
                region = excluded.region,
                district = excluded.district,
                property_type = excluded.property_type,
                trade_type = excluded.trade_type,
                price = excluded.price,
                area = excluded.area,
                floor = excluded.floor,
                building_name = excluded.building_name,
                description = excluded.description,
                is_urgent = excluded.is_urgent,
                tags = excluded.tags,
                confirmed_date = excluded.confirmed_date,
                crawled_at = excluded.crawled_at,
                crawl_session = excluded.crawl_session,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                naver_url = excluded.naver_url,
                price_sort_value = excluded.price_sort_value,
                rent_sort_value = excluded.rent_sort_value
        """

        with self.get_connection() as conn:
            sessions = conn.execute(
                """
                SELECT crawl_session, MAX(crawled_at) AS last_seen
                FROM listings
                WHERE crawl_session IS NOT NULL
                GROUP BY crawl_session
                ORDER BY last_seen DESC
                LIMIT 2
                """
            ).fetchall()
            if len(sessions) >= 2:
                old_session = sessions[-1]["crawl_session"]
                conn.execute("DELETE FROM listings WHERE crawl_session = ?", (old_session,))

            now = datetime.now().isoformat()
            region_stats_rows = self._build_region_stats_rows(session_id, listings, now)
            rows = []
            for listing in listings:
                price_value, rent_value = self._parse_price_sort_values(
                    listing.get("price"), listing.get("trade_type")
                )
                rows.append(
                    (
                        listing.get("article_no"),
                        listing.get("region"),
                        listing.get("district"),
                        listing.get("property_type"),
                        listing.get("trade_type"),
                        listing.get("price"),
                        listing.get("area"),
                        listing.get("floor"),
                        listing.get("building_name"),
                        listing.get("description"),
                        1 if listing.get("is_urgent") else 0,
                        json.dumps(listing.get("tags", []), ensure_ascii=False),
                        listing.get("confirmed_date"),
                        now,
                        session_id,
                        listing.get("latitude"),
                        listing.get("longitude"),
                        listing.get("naver_url"),
                        price_value,
                        rent_value,
                    )
                )

            if self.driver == "postgres":
                chunk_size = 500
                for start in range(0, len(rows), chunk_size):
                    conn.executemany(insert_sql, rows[start : start + chunk_size])
            else:
                for row in rows:
                    conn.execute(insert_sql, row)

            conn.execute("DELETE FROM crawl_region_stats WHERE session_id = ?", (session_id,))
            region_payload = [
                (
                    row["session_id"],
                    row["region"],
                    row["district"],
                    row["total_count"],
                    row["price_down_count"],
                    row["created_at"],
                )
                for row in region_stats_rows
            ]
            if region_payload:
                conn.executemany(
                    """
                    INSERT INTO crawl_region_stats
                    (session_id, region, district, total_count, price_down_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, region, district) DO UPDATE SET
                        total_count = excluded.total_count,
                        price_down_count = excluded.price_down_count,
                        created_at = excluded.created_at
                    """,
                    region_payload,
                )

            building_stats_rows = self._build_building_stats_rows(session_id, listings, now)
            building_payload = [
                (
                    row["session_id"],
                    row["region"],
                    row["district"],
                    row["building_name"],
                    row["total_count"],
                    row["price_down_count"],
                    row["created_at"],
                )
                for row in building_stats_rows
            ]
            if building_payload:
                conn.executemany(
                    """
                    INSERT INTO crawl_building_stats
                    (session_id, region, district, building_name, total_count, price_down_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, district, building_name) DO UPDATE SET
                        total_count = excluded.total_count,
                        price_down_count = excluded.price_down_count,
                        created_at = excluded.created_at
                    """,
                    building_payload,
                )

            building_stats_cutoff = (datetime.now() - timedelta(days=180)).isoformat()
            conn.execute(
                "DELETE FROM crawl_building_stats WHERE created_at < ?",
                (building_stats_cutoff,),
            )

    @staticmethod
    def _parse_tags(raw) -> List[str]:
        """tags 컬럼(JSON 문자열 또는 리스트)을 태그 문자열 리스트로 파싱한다."""
        if isinstance(raw, list):
            values = raw
        elif not raw:
            return []
        else:
            try:
                values = json.loads(raw)
            except (TypeError, ValueError):
                return []
            if not isinstance(values, list):
                return []
        return [str(value).strip() for value in values if str(value).strip()]

    def get_tag_counts(self):
        """최신 세션 매물의 태그별 등장 횟수를 count 내림차순으로 반환한다.

        SQLite JSON1 확장을 쓰지 않고(운영 DB는 Postgres) 애플리케이션 레벨에서 파싱한다.
        """
        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            if latest_session:
                rows = conn.execute(
                    "SELECT tags FROM listings WHERE crawl_session = ?",
                    (latest_session,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT tags FROM listings").fetchall()

        counter = Counter()
        for row in rows:
            counter.update(self._parse_tags(row["tags"]))

        return [
            {"tag": tag, "count": count}
            for tag, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        ]

    def get_listings(
        self,
        region="",
        district="",
        property_type="",
        trade_type="",
        urgent_only=False,
        search="",
        page=1,
        per_page=20,
        sort_by="urgent",
        price_down_only=False,
        tags=None,
    ):
        conditions = []
        params = []

        if region:
            conditions.append("region LIKE ?")
            params.append(f"%{region}%")
        if district:
            conditions.append("district LIKE ?")
            params.append(f"%{district}%")
        if property_type:
            if property_type == "__OTHER__":
                conditions.append("property_type NOT IN ('아파트','오피스텔','빌라/연립')")
            else:
                conditions.append("property_type = ?")
                params.append(property_type)
        if trade_type:
            conditions.append("trade_type = ?")
            params.append(trade_type)
        if urgent_only:
            conditions.append("is_urgent = 1")
        if price_down_only:
            conditions.append("tags LIKE '%가격인하%'")
        if tags:
            conditions.append("(" + " OR ".join(["tags LIKE ?"] * len(tags)) + ")")
            params.extend(f'%"{tag}"%' for tag in tags)
        if search:
            conditions.append(
                "(region LIKE ? OR district LIKE ? OR building_name LIKE ? OR description LIKE ?)"
            )
            params.extend([f"%{search}%"] * 4)

        order_map = {
            "urgent": "is_urgent DESC, crawled_at DESC",
            "recent": "crawled_at DESC",
            "price-asc": "CASE WHEN price_sort_value IS NULL THEN 1 ELSE 0 END, price_sort_value ASC, COALESCE(rent_sort_value, 0) ASC, crawled_at DESC",
            "price-desc": "CASE WHEN price_sort_value IS NULL THEN 1 ELSE 0 END, price_sort_value DESC, COALESCE(rent_sort_value, 0) DESC, crawled_at DESC",
        }
        order = order_map.get(sort_by, "is_urgent DESC, crawled_at DESC")

        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            session_params = [latest_session] if latest_session else []
            combined_conditions = list(conditions)
            if latest_session:
                combined_conditions.insert(0, "crawl_session = ?")

            scoped_where = (
                "WHERE " + " AND ".join(combined_conditions)
                if combined_conditions
                else ""
            )
            scoped_params = session_params + params

            total_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM listings {scoped_where}", scoped_params
            ).fetchone()
            total = total_row["count"] if total_row else 0

            price_down_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM listings {scoped_where} {'AND' if scoped_where else 'WHERE'} tags LIKE '%가격인하%'",
                scoped_params,
            ).fetchone()
            price_down = price_down_row["count"] if price_down_row else 0

            offset = (page - 1) * per_page
            rows = conn.execute(
                f"SELECT * FROM listings {scoped_where} ORDER BY {order} LIMIT ? OFFSET ?",
                scoped_params + [per_page, offset],
            ).fetchall()

            type_counts = {}
            for row in conn.execute(
                f"SELECT property_type, COUNT(*) as cnt FROM listings {scoped_where} GROUP BY property_type",
                scoped_params,
            ).fetchall():
                type_counts[row["property_type"]] = row["cnt"]

        return {
            "total": total,
            "urgent": total,
            "price_down_count": price_down,
            "type_counts": type_counts,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total + per_page - 1) // per_page),
            "listings": [dict(row) for row in rows],
        }

    def get_map_listings(
        self,
        min_lat=None,
        max_lat=None,
        min_lng=None,
        max_lng=None,
        region="",
        district="",
        property_type="",
        trade_type="",
        search="",
        price_down_only=False,
        tags=None,
        limit=500,
    ):
        """지도 화면 영역(bounds) 안의 좌표 보유 매물을 가벼운 필드로 반환한다."""
        conditions = [
            "latitude IS NOT NULL",
            "longitude IS NOT NULL",
        ]
        params = []

        if min_lat is not None:
            conditions.append("latitude >= ?")
            params.append(min_lat)
        if max_lat is not None:
            conditions.append("latitude <= ?")
            params.append(max_lat)
        if min_lng is not None:
            conditions.append("longitude >= ?")
            params.append(min_lng)
        if max_lng is not None:
            conditions.append("longitude <= ?")
            params.append(max_lng)
        if region:
            conditions.append("region LIKE ?")
            params.append(f"%{region}%")
        if district:
            conditions.append("district LIKE ?")
            params.append(f"%{district}%")
        if property_type:
            if property_type == "__OTHER__":
                conditions.append("property_type NOT IN ('아파트','오피스텔','빌라/연립')")
            else:
                conditions.append("property_type = ?")
                params.append(property_type)
        if trade_type:
            conditions.append("trade_type = ?")
            params.append(trade_type)
        if price_down_only:
            conditions.append("tags LIKE '%가격인하%'")
        if tags:
            conditions.append("(" + " OR ".join(["tags LIKE ?"] * len(tags)) + ")")
            params.extend(f'%"{tag}"%' for tag in tags)
        if search:
            conditions.append(
                "(region LIKE ? OR district LIKE ? OR building_name LIKE ? OR description LIKE ?)"
            )
            params.extend([f"%{search}%"] * 4)

        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            if latest_session:
                conditions.insert(0, "crawl_session = ?")
                params = [latest_session] + params

            rows = conn.execute(
                f"""
                SELECT id, article_no, building_name, price, latitude, longitude,
                       property_type, trade_type, tags, naver_url
                FROM listings
                WHERE {" AND ".join(conditions)}
                ORDER BY crawled_at DESC
                LIMIT ?
                """,
                params + [limit],
            ).fetchall()

        return [dict(row) for row in rows]

    def get_region_stats(self):
        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            params = [latest_session] if latest_session else []
            where = "WHERE crawl_session = ?" if latest_session else ""
            rows = conn.execute(
                f"""
                SELECT region, district,
                       CASE
                           WHEN region = district THEN region
                           ELSE region || ' ' || district
                       END as display_name,
                       COUNT(*) as total,
                       SUM(CASE WHEN tags LIKE '%가격인하%' THEN 1 ELSE 0 END) as price_down_count
                FROM listings
                {where}
                GROUP BY region, district
                ORDER BY total DESC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def get_trends(self):
        with self.get_connection() as conn:
            latest_session_row = conn.execute(
                """
                SELECT session_id, DATE(crawled_at) AS crawl_date
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
                ORDER BY crawled_at DESC
                LIMIT 1
                """
            ).fetchone()

            latest_session = latest_session_row["session_id"] if latest_session_row else None
            latest_date = latest_session_row["crawl_date"] if latest_session_row else None
            if not latest_date:
                return []

            if isinstance(latest_date, datetime):
                latest_date = latest_date.date().isoformat()
            elif not isinstance(latest_date, str):
                latest_date = str(latest_date)

            prev_date = (date.fromisoformat(latest_date) - timedelta(days=1)).isoformat()

            prev_session_row = conn.execute(
                """
                SELECT session_id
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
                  AND DATE(crawled_at) = ?
                ORDER BY crawled_at DESC
                LIMIT 1
                """,
                (prev_date,),
            ).fetchone()
            prev_session = prev_session_row["session_id"] if prev_session_row else None

            rows = conn.execute(
                """
                WITH
                curr AS (
                    SELECT region, district, total_count AS cnt, price_down_count
                    FROM crawl_region_stats
                    WHERE session_id = ?
                ),
                prev AS (
                    SELECT region, district, total_count AS cnt
                    FROM crawl_region_stats
                    WHERE session_id = ?
                ),
                keys AS (
                    SELECT region, district FROM curr
                    UNION
                    SELECT region, district FROM prev
                )
                SELECT k.region,
                       k.district,
                       CASE
                           WHEN k.region = k.district THEN k.region
                           ELSE k.region || ' ' || k.district
                       END as display_name,
                       COALESCE(c.cnt, 0) as current_cnt,
                       COALESCE(p.cnt, 0) as prev_cnt,
                       COALESCE(c.cnt, 0) - COALESCE(p.cnt, 0) as diff,
                       COALESCE(c.price_down_count, 0) as price_down_count,
                       ? as current_date,
                       ? as previous_date
                FROM keys k
                LEFT JOIN curr c ON k.region = c.region AND k.district = c.district
                LEFT JOIN prev p ON k.region = p.region AND k.district = p.district
                ORDER BY diff DESC, current_cnt DESC, display_name ASC
                """,
                (latest_session, prev_session or "", latest_date, prev_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_building_stats_history(self, district: str, building_name: str, limit: int = 90):
        limit = max(1, int(limit or 90))
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT cb.session_id, cb.total_count, cb.price_down_count, ch.crawled_at
                FROM crawl_building_stats cb
                JOIN crawl_history ch ON ch.session_id = cb.session_id
                WHERE cb.district = ? AND cb.building_name = ?
                  AND ch.status = 'success'
                  AND COALESCE(ch.source, 'naver') <> 'demo'
                ORDER BY ch.crawled_at DESC
                LIMIT ?
                """,
                (district, building_name, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def _get_latest_building_changes(
        self,
        conn: ConnectionWrapper,
        buildings: Sequence[Tuple[str, str]],
    ):
        keys = list(
            dict.fromkeys(
                (str(district or "").strip(), str(building_name or "").strip())
                for district, building_name in buildings
                if str(district or "").strip() and str(building_name or "").strip()
            )
        )
        if not keys:
            return {}

        key_conditions = " OR ".join(
            ["(cb.district = ? AND cb.building_name = ?)"] * len(keys)
        )
        params = [value for key in keys for value in key]
        rows = conn.execute(
            f"""
            WITH live_sessions AS (
                SELECT session_id,
                       crawled_at,
                       ROW_NUMBER() OVER (ORDER BY crawled_at DESC) AS session_rank
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
            ),
            ranked AS (
                SELECT cb.district,
                       cb.building_name,
                       cb.total_count,
                       cb.price_down_count,
                       cb.session_id,
                       ls.crawled_at,
                       ls.session_rank AS snapshot_rank
                FROM crawl_building_stats cb
                JOIN live_sessions ls ON ls.session_id = cb.session_id
                WHERE ls.session_rank <= 2
                  AND ({key_conditions})
            )
            SELECT *
            FROM ranked
            WHERE snapshot_rank <= 2
            ORDER BY district, building_name, snapshot_rank
            """,
            params,
        ).fetchall()

        grouped = {}
        for row in rows:
            key = (row["district"], row["building_name"])
            grouped.setdefault(key, []).append(row)

        changes = {}
        for key, snapshots in grouped.items():
            if len(snapshots) < 2:
                continue
            current, previous = snapshots[0], snapshots[1]
            current_total = int(current["total_count"] or 0)
            previous_total = int(previous["total_count"] or 0)
            current_price_down = int(current["price_down_count"] or 0)
            previous_price_down = int(previous["price_down_count"] or 0)
            changes[key] = {
                "current_session_id": current["session_id"],
                "previous_session_id": previous["session_id"],
                "current_crawled_at": current["crawled_at"],
                "previous_crawled_at": previous["crawled_at"],
                "current_total": current_total,
                "previous_total": previous_total,
                "total_diff": current_total - previous_total,
                "current_price_down": current_price_down,
                "previous_price_down": previous_price_down,
                "price_down_diff": current_price_down - previous_price_down,
            }
        return changes

    def get_latest_building_changes(self, buildings: Sequence[Tuple[str, str]]):
        """여러 단지의 최신/직전 일별 스냅샷을 한 쿼리로 반환한다."""
        with self.get_connection() as conn:
            return self._get_latest_building_changes(conn, buildings)

    def get_last_crawl(self, prefer_visible: bool = False):
        with self.get_connection() as conn:
            row = None
            if prefer_visible:
                row = conn.execute(
                    """
                    SELECT *
                    FROM crawl_history
                    WHERE status = 'success'
                      AND COALESCE(source, 'naver') <> 'demo'
                    ORDER BY crawled_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT * FROM crawl_history ORDER BY crawled_at DESC LIMIT 1"
                ).fetchone()
        return dict(row) if row else None

    def get_last_successful_live_crawl(self):
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
                ORDER BY crawled_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def get_recent_successful_crawls(self, limit: int = 90):
        limit = max(1, int(limit or 90))
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
                ORDER BY crawled_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def log_crawl(self, session_id, total_count, urgent_count, status, source):
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    datetime.now().isoformat(),
                    total_count,
                    urgent_count,
                    status,
                    source,
                ),
            )
