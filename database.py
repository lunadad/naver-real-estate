import os
import json
import sqlite3
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

try:
    import psycopg
except ImportError:  # pragma: no cover - optional for sqlite-only use
    psycopg = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - optional for sqlite-only use
    ConnectionPool = None


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
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if self.release:
                self.release(exc_type, exc, tb)
            else:
                self.conn.close()

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
        self.database_url = (database_url or "").strip()
        self.driver = "postgres" if self.database_url else "sqlite"
        self.skip_price_backfill = skip_price_backfill
        self.connect_timeout = int((os.getenv("PGCONNECT_TIMEOUT") or "10").strip())
        self.pool = None
        if self.driver == "postgres" and ConnectionPool is not None:
            self.pool = ConnectionPool(
                self.database_url,
                kwargs={"connect_timeout": self.connect_timeout},
                min_size=int((os.getenv("DB_POOL_MIN_SIZE") or "1").strip()),
                max_size=int((os.getenv("DB_POOL_MAX_SIZE") or "5").strip()),
                timeout=float((os.getenv("DB_POOL_TIMEOUT") or "10").strip()),
                open=True,
            )
        self.init_db()

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

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_sort ON listings(price_sort_value)"
            )
            if not self.skip_price_backfill:
                self._backfill_price_sort_values(conn)

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

            CREATE TABLE IF NOT EXISTS alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id);
            CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id);
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
            CREATE TABLE IF NOT EXISTS alert_rules (
                id BIGSERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                keyword TEXT,
                district TEXT,
                property_type TEXT,
                trade_type TEXT,
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
            "CREATE INDEX IF NOT EXISTS idx_region ON listings(region)",
            "CREATE INDEX IF NOT EXISTS idx_district ON listings(district)",
            "CREATE INDEX IF NOT EXISTS idx_property_type ON listings(property_type)",
            "CREATE INDEX IF NOT EXISTS idx_is_urgent ON listings(is_urgent)",
            "CREATE INDEX IF NOT EXISTS idx_crawled_at ON listings(crawled_at)",
            "CREATE INDEX IF NOT EXISTS idx_session ON listings(crawl_session)",
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_client_id ON alert_rules(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_id ON alert_deliveries(alert_id)",
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
        return " · ".join(parts) if parts else "전체 급매 알림"

    def create_alert_rule(
        self,
        client_id: str,
        keyword: str = "",
        district: str = "",
        property_type: str = "",
        trade_type: str = "",
        name: str = "",
    ):
        client_id = self._normalize_alert_value(client_id)
        keyword = self._normalize_alert_value(keyword)
        district = self._normalize_alert_value(district)
        property_type = self._normalize_alert_value(property_type)
        trade_type = self._normalize_alert_value(trade_type)
        name = self._normalize_alert_value(name) or self._build_alert_name(
            keyword, district, property_type, trade_type
        )

        with self.get_connection() as conn:
            if self.driver == "postgres":
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type, enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                    RETURNING id
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
                        datetime.now().isoformat(),
                    ),
                )
                rule_id = cursor.fetchone()["id"]
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO alert_rules
                    (client_id, name, keyword, district, property_type, trade_type, enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        client_id,
                        name,
                        keyword,
                        district,
                        property_type,
                        trade_type,
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
            latest_date_row = conn.execute(
                """
                SELECT DATE(MAX(crawled_at)) AS latest_date
                FROM crawl_history
                WHERE status = 'success'
                  AND COALESCE(source, 'naver') <> 'demo'
                """
            ).fetchone()

            latest_date = latest_date_row["latest_date"] if latest_date_row else None
            if not latest_date:
                return []

            if isinstance(latest_date, datetime):
                latest_date = latest_date.date().isoformat()
            elif not isinstance(latest_date, str):
                latest_date = str(latest_date)

            prev_date = (date.fromisoformat(latest_date) - timedelta(days=1)).isoformat()

            rows = conn.execute(
                """
                WITH visible AS (
                    SELECT l.region, l.district, l.tags, DATE(h.crawled_at) AS crawl_date
                    FROM listings l
                    INNER JOIN crawl_history h ON h.session_id = l.crawl_session
                    WHERE h.status = 'success'
                      AND COALESCE(h.source, 'naver') <> 'demo'
                ),
                curr AS (
                    SELECT region, district, COUNT(*) as cnt
                    FROM visible
                    WHERE crawl_date = ?
                    GROUP BY region, district
                ),
                prev AS (
                    SELECT region, district, COUNT(*) as cnt
                    FROM visible
                    WHERE crawl_date = ?
                    GROUP BY region, district
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
                       COALESCE((
                           SELECT SUM(CASE WHEN tags LIKE '%가격인하%' THEN 1 ELSE 0 END)
                           FROM visible l
                           WHERE l.region = k.region
                             AND l.district = k.district
                             AND l.crawl_date = ?
                       ), 0) as price_down_count,
                       ? as current_date,
                       ? as previous_date
                FROM keys k
                LEFT JOIN curr c ON k.region = c.region AND k.district = c.district
                LEFT JOIN prev p ON k.region = p.region AND k.district = p.district
                ORDER BY diff DESC, current_cnt DESC, display_name ASC
                """,
                (latest_date, prev_date, latest_date, latest_date, prev_date),
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
