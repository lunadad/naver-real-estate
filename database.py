import os
import json
import logging
import sqlite3
import re
from datetime import date, datetime, timedelta
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

COMMERCIAL_PROPERTY_TYPES = ("상가", "업무", "토지")
PROPERTY_CODE_BY_TYPE = {
    "상가": "OBYG",
    "업무": "SGJT",
    "토지": "TJ",
}
DEFAULT_POSTGRES_SCHEMA = "commercial_v2"


def normalize_postgres_identifier(value: str) -> str:
    identifier = (value or "").strip()
    if not identifier:
        return ""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid Postgres identifier: {identifier}")
    return identifier


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


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
        self.postgres_schema = normalize_postgres_identifier(
            os.getenv("DB_SCHEMA", DEFAULT_POSTGRES_SCHEMA)
        )
        self.pool = None
        self._open_pool()
        self.init_db()

    def _open_pool(self):
        if self.driver != "postgres" or ConnectionPool is None:
            return

        pool_kwargs = {
            "kwargs": {"connect_timeout": self.connect_timeout},
            "min_size": int((os.getenv("DB_POOL_MIN_SIZE") or "0").strip()),
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
                self._configure_postgres_connection(conn)
                return ConnectionWrapper(
                    "postgres",
                    conn,
                    release=lambda exc_type, exc, tb: pool_conn.__exit__(
                        exc_type, exc, tb
                    ),
                )

            conn = psycopg.connect(self.database_url, connect_timeout=self.connect_timeout)
            self._configure_postgres_connection(conn)
            return ConnectionWrapper("postgres", conn)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return ConnectionWrapper("sqlite", conn)

    def _configure_postgres_connection(self, conn):
        if self.driver != "postgres" or not self.postgres_schema:
            return

        schema = quote_identifier(self.postgres_schema)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"SET search_path TO {schema}, public")

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
            if "raw_property_code" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN raw_property_code TEXT")
            if "area_m2" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN area_m2 DOUBLE PRECISION")
            if "land_use_zone" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN land_use_zone TEXT")
            if "land_category" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN land_category TEXT")
            if "road_access" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN road_access TEXT")
            if "premium_info" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN premium_info TEXT")
            if "estimated_yield_rate" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN estimated_yield_rate DOUBLE PRECISION")
            if "price_drop_rate" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN price_drop_rate DOUBLE PRECISION")

            alert_cols = self._get_table_columns(conn, "alert_rules")
            if "min_area_m2" not in alert_cols:
                conn.execute("ALTER TABLE alert_rules ADD COLUMN min_area_m2 DOUBLE PRECISION")
            if "max_area_m2" not in alert_cols:
                conn.execute("ALTER TABLE alert_rules ADD COLUMN max_area_m2 DOUBLE PRECISION")
            if "trade_scope" not in alert_cols:
                conn.execute("ALTER TABLE alert_rules ADD COLUMN trade_scope TEXT")
            if "min_price_drop_rate" not in alert_cols:
                conn.execute("ALTER TABLE alert_rules ADD COLUMN min_price_drop_rate DOUBLE PRECISION")

            self._import_public_commercial_data(conn)

            if self.skip_price_backfill:
                logger.info("Startup listing index maintenance skipped")
                logger.info("Startup listing backfill skipped")
            else:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_price_sort ON listings(price_sort_value)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_raw_property_code ON listings(raw_property_code)"
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_area_m2 ON listings(area_m2)")
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_price_drop_rate ON listings(price_drop_rate)"
                )
                conn.execute("UPDATE listings SET property_type = '상가' WHERE property_type = '상가/업무'")
                self._backfill_price_sort_values(conn)
                self._backfill_commercial_metadata(conn)
            latest_visible_session = self._get_latest_visible_session_id(conn)
            if latest_visible_session and not self.skip_price_backfill:
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
                rent_sort_value INTEGER,
                raw_property_code TEXT,
                area_m2 REAL,
                land_use_zone TEXT,
                land_category TEXT,
                road_access TEXT,
                premium_info TEXT,
                estimated_yield_rate REAL,
                price_drop_rate REAL
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

            CREATE TABLE IF NOT EXISTS alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                min_area_m2 REAL,
                max_area_m2 REAL,
                trade_scope TEXT,
                min_price_drop_rate REAL,
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
            CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id);
            CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id);
            CREATE INDEX IF NOT EXISTS idx_push_subscriptions_client_id ON push_subscriptions(client_id);
            """
        )

    def _init_postgres(self, conn: ConnectionWrapper):
        table_statements = [
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
                rent_sort_value BIGINT,
                raw_property_code TEXT,
                area_m2 DOUBLE PRECISION,
                land_use_zone TEXT,
                land_category TEXT,
                road_access TEXT,
                premium_info TEXT,
                estimated_yield_rate DOUBLE PRECISION,
                price_drop_rate DOUBLE PRECISION
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
            CREATE TABLE IF NOT EXISTS alert_rules (
                id BIGSERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
                min_area_m2 DOUBLE PRECISION,
                max_area_m2 DOUBLE PRECISION,
                trade_scope TEXT,
                min_price_drop_rate DOUBLE PRECISION,
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
        ]
        index_statements = [
            "CREATE INDEX IF NOT EXISTS idx_region ON listings(region)",
            "CREATE INDEX IF NOT EXISTS idx_district ON listings(district)",
            "CREATE INDEX IF NOT EXISTS idx_property_type ON listings(property_type)",
            "CREATE INDEX IF NOT EXISTS idx_is_urgent ON listings(is_urgent)",
            "CREATE INDEX IF NOT EXISTS idx_crawled_at ON listings(crawled_at)",
            "CREATE INDEX IF NOT EXISTS idx_session ON listings(crawl_session)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_region_stats_session ON crawl_region_stats(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id)",
            "CREATE INDEX IF NOT EXISTS idx_push_subscriptions_client_id ON push_subscriptions(client_id)",
        ]
        statements = table_statements
        if self.skip_price_backfill:
            logger.info("Startup index maintenance skipped")
        else:
            statements = table_statements + index_statements

        for statement in statements:
            conn.execute(statement)

    def _get_table_columns(self, conn: ConnectionWrapper, table_name: str):
        if self.driver == "postgres":
            rows = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = ?
                """,
                (table_name,),
            ).fetchall()
            return {row["column_name"] for row in rows}

        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def _public_table_exists(self, conn: ConnectionWrapper, table_name: str) -> bool:
        if self.driver != "postgres" or self.postgres_schema == "public":
            return False

        row = conn.execute(
            """
            SELECT 1 AS exists
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return bool(row)

    def _import_public_commercial_data(self, conn: ConnectionWrapper):
        if self.driver != "postgres" or self.postgres_schema == "public":
            return
        if not self._public_table_exists(conn, "listings"):
            return

        commercial_type_params = tuple(COMMERCIAL_PROPERTY_TYPES)
        placeholders = ", ".join(["?"] * len(commercial_type_params))

        conn.execute(
            f"""
            INSERT INTO listings
            (article_no, region, district, property_type, trade_type, price,
             area, floor, building_name, description, is_urgent, tags,
             confirmed_date, crawled_at, crawl_session, latitude, longitude, naver_url,
             price_sort_value, rent_sort_value, raw_property_code, area_m2,
             land_use_zone, land_category, road_access, premium_info,
             estimated_yield_rate, price_drop_rate)
            SELECT article_no, region, district, property_type, trade_type, price,
                   area, floor, building_name, description, is_urgent, tags,
                   confirmed_date, crawled_at, crawl_session, latitude, longitude, naver_url,
                   price_sort_value, rent_sort_value, raw_property_code, area_m2,
                   land_use_zone, land_category, road_access, premium_info,
                   estimated_yield_rate, price_drop_rate
            FROM public.listings
            WHERE property_type IN ({placeholders})
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
                rent_sort_value = excluded.rent_sort_value,
                raw_property_code = excluded.raw_property_code,
                area_m2 = excluded.area_m2,
                land_use_zone = excluded.land_use_zone,
                land_category = excluded.land_category,
                road_access = excluded.road_access,
                premium_info = excluded.premium_info,
                estimated_yield_rate = excluded.estimated_yield_rate,
                price_drop_rate = excluded.price_drop_rate
            """,
            commercial_type_params,
        )

        if self._public_table_exists(conn, "crawl_history"):
            conn.execute(
                f"""
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                SELECT h.session_id, h.crawled_at, h.total_count, h.urgent_count, h.status, h.source
                FROM public.crawl_history h
                WHERE h.session_id IN (
                    SELECT DISTINCT crawl_session
                    FROM public.listings
                    WHERE crawl_session IS NOT NULL
                      AND property_type IN ({placeholders})
                )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM crawl_history existing
                    WHERE existing.session_id = h.session_id
                  )
                """,
                commercial_type_params,
            )

        if self._public_table_exists(conn, "crawl_region_stats"):
            conn.execute(
                f"""
                INSERT INTO crawl_region_stats
                (session_id, region, district, total_count, price_down_count, created_at)
                SELECT s.session_id, s.region, s.district, s.total_count, s.price_down_count, s.created_at
                FROM public.crawl_region_stats s
                WHERE s.session_id IN (
                    SELECT DISTINCT crawl_session
                    FROM public.listings
                    WHERE crawl_session IS NOT NULL
                      AND property_type IN ({placeholders})
                )
                ON CONFLICT(session_id, region, district) DO UPDATE SET
                    total_count = excluded.total_count,
                    price_down_count = excluded.price_down_count,
                    created_at = excluded.created_at
                """,
                commercial_type_params,
            )

            conn.execute(
                f"""
                DELETE FROM public.crawl_region_stats
                WHERE session_id IN (
                    SELECT DISTINCT crawl_session
                    FROM public.listings
                    WHERE crawl_session IS NOT NULL
                      AND property_type IN ({placeholders})
                )
                """,
                commercial_type_params,
            )

        if self._public_table_exists(conn, "crawl_history"):
            conn.execute(
                f"""
                DELETE FROM public.crawl_history
                WHERE session_id IN (
                    SELECT DISTINCT crawl_session
                    FROM public.listings
                    WHERE crawl_session IS NOT NULL
                      AND property_type IN ({placeholders})
                )
                """,
                commercial_type_params,
            )

        conn.execute(
            f"""
            DELETE FROM public.listings
            WHERE property_type IN ({placeholders})
            """,
            commercial_type_params,
        )

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

    def _parse_area_to_m2(self, raw: Optional[str]) -> Optional[float]:
        text = str(raw or "").strip()
        if not text:
            return None

        normalized = text.replace(",", "")
        match = re.search(r"(\d+(?:\.\d+)?)", normalized)
        if not match:
            return None

        value = float(match.group(1))
        if "평" in normalized and "㎡" not in normalized and "m2" not in normalized.lower():
            value *= 3.305785
        return round(value, 2)

    def _coerce_float(self, value) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(str(value).replace("%", "").replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    def _extract_tags(self, tags) -> List[str]:
        if isinstance(tags, list):
            return [str(tag) for tag in tags if str(tag).strip()]
        if isinstance(tags, str):
            try:
                parsed = json.loads(tags)
                if isinstance(parsed, list):
                    return [str(tag) for tag in parsed if str(tag).strip()]
            except Exception:
                return [tags]
        return []

    def _infer_premium_info(self, tags: Sequence[str], description: Optional[str]) -> Optional[str]:
        text = " ".join([*(tags or []), str(description or "")])
        if not text:
            return None
        if any(token in text for token in ("무권리", "권리금 없음", "권리금없음")):
            return "무권리"
        if "권리금" in text:
            return "권리금 확인"
        return None

    def _infer_road_access(self, tags: Sequence[str], description: Optional[str]) -> Optional[str]:
        text = " ".join([*(tags or []), str(description or "")])
        if not text:
            return None
        if any(token in text for token in ("코너", "양면", "삼면")):
            return "코너/다중접면"
        if any(token in text for token in ("대로변", "대로", "왕복", "광로")):
            return "대로변"
        if any(token in text for token in ("도로접", "접도", "도로 접", "진입로")):
            return "도로접면"
        if "맹지" in text:
            return "맹지 유의"
        return None

    def _infer_land_use_zone(self, tags: Sequence[str], description: Optional[str]) -> Optional[str]:
        text = " ".join([*(tags or []), str(description or "")])
        patterns = [
            "계획관리",
            "생산관리",
            "보전관리",
            "자연녹지",
            "생산녹지",
            "보전녹지",
            "일반상업",
            "중심상업",
            "근린상업",
            "준주거",
            "제1종일반주거",
            "제2종일반주거",
            "제3종일반주거",
            "일반주거",
            "전용주거",
            "공업지역",
            "농림지역",
        ]
        for pattern in patterns:
            if pattern in text:
                return pattern
        return None

    def _infer_land_category(self, tags: Sequence[str], description: Optional[str]) -> Optional[str]:
        text = " ".join([*(tags or []), str(description or "")])
        for pattern in ("대지", "전", "답", "잡종지", "임야", "공장용지", "도로"):
            if pattern in text:
                return pattern
        return None

    def _estimate_yield_rate(
        self,
        price: Optional[str],
        trade_type: Optional[str],
        price_value: Optional[int],
        rent_value: Optional[int],
    ) -> Optional[float]:
        deposit_value, monthly_value = self._parse_price_sort_values(price, trade_type)
        if monthly_value is None or monthly_value <= 0:
            return None

        denominator = price_value or deposit_value
        if not denominator or denominator <= 0:
            return None

        return round((monthly_value * 12 / denominator) * 100, 2)

    def _build_listing_metadata(self, listing: Dict) -> Dict:
        tags = self._extract_tags(listing.get("tags"))
        property_type = str(listing.get("property_type") or "").strip()
        price_value, rent_value = self._parse_price_sort_values(
            listing.get("price"), listing.get("trade_type")
        )
        metadata = {
            "raw_property_code": (
                str(listing.get("raw_property_code") or "").strip()
                or PROPERTY_CODE_BY_TYPE.get(property_type)
            ),
            "area_m2": self._coerce_float(listing.get("area_m2"))
            or self._parse_area_to_m2(listing.get("area")),
            "land_use_zone": self._normalize_alert_value(listing.get("land_use_zone")),
            "land_category": self._normalize_alert_value(listing.get("land_category")),
            "road_access": self._normalize_alert_value(listing.get("road_access")),
            "premium_info": self._normalize_alert_value(listing.get("premium_info")),
            "estimated_yield_rate": self._coerce_float(listing.get("estimated_yield_rate")),
            "price_drop_rate": self._coerce_float(listing.get("price_drop_rate")),
        }

        if not metadata["premium_info"]:
            metadata["premium_info"] = self._infer_premium_info(tags, listing.get("description"))
        if not metadata["road_access"]:
            metadata["road_access"] = self._infer_road_access(tags, listing.get("description"))
        if property_type == "토지":
            if not metadata["land_use_zone"]:
                metadata["land_use_zone"] = self._infer_land_use_zone(tags, listing.get("description"))
            if not metadata["land_category"]:
                metadata["land_category"] = self._infer_land_category(tags, listing.get("description"))
        if metadata["estimated_yield_rate"] is None:
            metadata["estimated_yield_rate"] = self._estimate_yield_rate(
                listing.get("price"),
                listing.get("trade_type"),
                price_value,
                rent_value,
            )
        return metadata

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

    def _backfill_commercial_metadata(self, conn: ConnectionWrapper):
        rows = conn.execute(
            """
            SELECT id, property_type, price, trade_type, area, description, tags,
                   raw_property_code, area_m2, land_use_zone, land_category,
                   road_access, premium_info, estimated_yield_rate, price_drop_rate
            FROM listings
            WHERE raw_property_code IS NULL
               OR area_m2 IS NULL
               OR premium_info IS NULL
               OR road_access IS NULL
               OR (property_type = '토지' AND (land_use_zone IS NULL OR land_category IS NULL))
            """
        ).fetchall()

        for row in rows:
            metadata = self._build_listing_metadata(dict(row))
            conn.execute(
                """
                UPDATE listings
                SET raw_property_code = ?,
                    area_m2 = ?,
                    land_use_zone = COALESCE(?, land_use_zone),
                    land_category = COALESCE(?, land_category),
                    road_access = COALESCE(?, road_access),
                    premium_info = COALESCE(?, premium_info),
                    estimated_yield_rate = COALESCE(?, estimated_yield_rate),
                    price_drop_rate = COALESCE(?, price_drop_rate)
                WHERE id = ?
                """,
                (
                    metadata["raw_property_code"],
                    metadata["area_m2"],
                    metadata["land_use_zone"],
                    metadata["land_category"],
                    metadata["road_access"],
                    metadata["premium_info"],
                    metadata["estimated_yield_rate"],
                    metadata["price_drop_rate"],
                    row["id"],
                ),
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

    def _sanitize_naver_url(self, url: Optional[str], source: Optional[str] = None) -> str:
        value = self._normalize_alert_value(url)
        if not value:
            return ""
        if source == "demo":
            return ""
        if "new.land.naver.com/search?query=" in value:
            return ""
        if "new.land.naver.com" in value and "articleNo=" not in value:
            return ""
        return value

    def _normalize_alert_value(self, value: Optional[str]) -> str:
        return str(value or "").strip()

    def _build_alert_name(
        self,
        keyword: str,
        district: str,
        property_type: str,
        trade_type: str,
        min_area_m2: Optional[float] = None,
        max_area_m2: Optional[float] = None,
        trade_scope: str = "",
        min_price_drop_rate: Optional[float] = None,
    ) -> str:
        parts = []
        if keyword:
            parts.append(keyword)
        if district:
            parts.append(district)
        if property_type:
            parts.append(property_type)
        if trade_type:
            parts.append(trade_type)
        if trade_scope == "sale":
            parts.append("매매 전용")
        if trade_scope == "rent":
            parts.append("임대 전용")
        if min_area_m2 is not None or max_area_m2 is not None:
            if min_area_m2 is not None and max_area_m2 is not None:
                parts.append(f"{min_area_m2:g}-{max_area_m2:g}㎡")
            elif min_area_m2 is not None:
                parts.append(f"{min_area_m2:g}㎡ 이상")
            else:
                parts.append(f"{max_area_m2:g}㎡ 이하")
        if min_price_drop_rate is not None:
            parts.append(f"가격인하율 {min_price_drop_rate:g}%+")
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
        min_area_m2: Optional[float] = None,
        max_area_m2: Optional[float] = None,
        trade_scope: str = "",
        min_price_drop_rate: Optional[float] = None,
        name: str = "",
    ):
        client_id = self._normalize_alert_value(client_id)
        keyword = self._normalize_alert_value(keyword)
        district = self._normalize_alert_value(district)
        property_type = self._normalize_alert_value(property_type)
        trade_type = self._normalize_alert_value(trade_type)
        trade_scope = self._normalize_alert_value(trade_scope)
        if trade_scope not in {"", "sale", "rent"}:
            trade_scope = ""
        min_area_m2 = self._coerce_float(min_area_m2)
        max_area_m2 = self._coerce_float(max_area_m2)
        min_price_drop_rate = self._coerce_float(min_price_drop_rate)
        name = self._normalize_alert_value(name) or self._build_alert_name(
            keyword,
            district,
            property_type,
            trade_type,
            min_area_m2,
            max_area_m2,
            trade_scope,
            min_price_drop_rate,
        )

        with self.get_connection() as conn:
            if self.driver == "postgres":
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type,
                     min_area_m2, max_area_m2, trade_scope, min_price_drop_rate,
                     enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    RETURNING id
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
                        min_area_m2,
                        max_area_m2,
                        trade_scope,
                        min_price_drop_rate,
                        datetime.now().isoformat(),
                    ),
                )
                rule_id = cursor.fetchone()["id"]
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type,
                     min_area_m2, max_area_m2, trade_scope, min_price_drop_rate,
                     enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
                        min_area_m2,
                        max_area_m2,
                        trade_scope,
                        min_price_drop_rate,
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
            ORDER BY created_at DESC
            """,
            (client_id,),
        ).fetchall()

        matches_by_article = {}
        for rule in rules:
            conditions = [
                "crawl_session = ?",
                "crawled_at >= ?",
                "property_type IN ('상가', '업무', '토지')",
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
            if rule.get("trade_scope") == "sale":
                conditions.append("trade_type = '매매'")
            elif rule.get("trade_scope") == "rent":
                conditions.append("trade_type IN ('전세', '월세')")
            min_area_m2 = self._coerce_float(rule.get("min_area_m2"))
            max_area_m2 = self._coerce_float(rule.get("max_area_m2"))
            min_price_drop_rate = self._coerce_float(rule.get("min_price_drop_rate"))
            if min_area_m2 is not None:
                conditions.append("area_m2 IS NOT NULL AND area_m2 >= ?")
                params.append(min_area_m2)
            if max_area_m2 is not None:
                conditions.append("area_m2 IS NOT NULL AND area_m2 <= ?")
                params.append(max_area_m2)
            if min_price_drop_rate is not None:
                conditions.append("price_drop_rate IS NOT NULL AND price_drop_rate >= ?")
                params.append(min_price_drop_rate)

            rows = conn.execute(
                f"""
                SELECT listings.*,
                       (
                         SELECT source
                         FROM crawl_history h
                         WHERE h.session_id = listings.crawl_session
                         ORDER BY h.crawled_at DESC
                         LIMIT 1
                       ) AS crawl_source
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
                    source = entry.pop("crawl_source", None)
                    entry["naver_url"] = self._sanitize_naver_url(
                        entry.get("naver_url"), source
                    )
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
        source = item.pop("crawl_source", None)
        item["naver_url"] = self._sanitize_naver_url(item.get("naver_url"), source)
        return item

    def get_pending_alert_matches(self, client_id: str, limit: int = 10):
        with self.get_connection() as conn:
            return self._collect_alert_matches(conn, client_id, limit)

    def mark_alert_matches_delivered(self, matches: List[Dict]):
        delivery_refs = []
        for match in matches:
            delivery_refs.extend(match.get("_delivery_refs", []))

        if not delivery_refs:
            return

        with self.get_connection() as conn:
            self._mark_delivery_refs(conn, delivery_refs)

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
             price_sort_value, rent_sort_value, raw_property_code, area_m2,
             land_use_zone, land_category, road_access, premium_info,
             estimated_yield_rate, price_drop_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                rent_sort_value = excluded.rent_sort_value,
                raw_property_code = excluded.raw_property_code,
                area_m2 = excluded.area_m2,
                land_use_zone = excluded.land_use_zone,
                land_category = excluded.land_category,
                road_access = excluded.road_access,
                premium_info = excluded.premium_info,
                estimated_yield_rate = excluded.estimated_yield_rate,
                price_drop_rate = excluded.price_drop_rate
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
                metadata = self._build_listing_metadata(listing)
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
                        metadata["raw_property_code"],
                        metadata["area_m2"],
                        metadata["land_use_zone"],
                        metadata["land_category"],
                        metadata["road_access"],
                        metadata["premium_info"],
                        metadata["estimated_yield_rate"],
                        metadata["price_drop_rate"],
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
    ):
        conditions = ["property_type IN ('상가', '업무', '토지')"]
        params = []

        if region:
            conditions.append("region LIKE ?")
            params.append(f"%{region}%")
        if district:
            conditions.append("district LIKE ?")
            params.append(f"%{district}%")
        if property_type:
            conditions.append("property_type = ?")
            params.append(property_type)
        if trade_type:
            conditions.append("trade_type = ?")
            params.append(trade_type)
        if urgent_only:
            conditions.append("is_urgent = 1")
        if price_down_only:
            conditions.append("tags LIKE '%가격인하%'")
        if search:
            conditions.append(
                "(region LIKE ? OR district LIKE ? OR building_name LIKE ? OR description LIKE ? OR tags LIKE ? OR land_use_zone LIKE ? OR land_category LIKE ? OR road_access LIKE ? OR premium_info LIKE ?)"
            )
            params.extend([f"%{search}%"] * 9)

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
                f"""
                SELECT listings.*,
                       (
                         SELECT source
                         FROM crawl_history h
                         WHERE h.session_id = listings.crawl_session
                         ORDER BY h.crawled_at DESC
                         LIMIT 1
                       ) AS crawl_source
                FROM listings
                {scoped_where}
                ORDER BY {order}
                LIMIT ? OFFSET ?
                """,
                scoped_params + [per_page, offset],
            ).fetchall()

            type_counts = {}
            for row in conn.execute(
                f"SELECT property_type, COUNT(*) as cnt FROM listings {scoped_where} GROUP BY property_type",
                scoped_params,
            ).fetchall():
                type_counts[row["property_type"]] = row["cnt"]

        listings_payload = []
        for row in rows:
            item = dict(row)
            source = item.pop("crawl_source", None)
            item["naver_url"] = self._sanitize_naver_url(item.get("naver_url"), source)
            listings_payload.append(item)

        return {
            "total": total,
            "urgent": total,
            "price_down_count": price_down,
            "type_counts": type_counts,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total + per_page - 1) // per_page),
            "listings": listings_payload,
        }

    def get_region_stats(self):
        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            params = [latest_session] if latest_session else []
            conditions = ["property_type IN ('상가', '업무', '토지')"]
            if latest_session:
                conditions.insert(0, "crawl_session = ?")
            where = "WHERE " + " AND ".join(conditions)
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

    def count_commercial_listings_for_session(self, session_id: str) -> int:
        if not session_id:
            return 0

        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM listings
                WHERE crawl_session = ?
                  AND property_type IN ('상가', '업무', '토지')
                """,
                (session_id,),
            ).fetchone()
        return int((row or {}).get("count") or 0)

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
