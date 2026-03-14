import sqlite3
import json
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple


class Database:
    def __init__(self, db_path="real_estate.db"):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            conn.executescript("""
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
                -- 기존 DB에 컬럼이 없으면 추가 (마이그레이션)
                CREATE TABLE IF NOT EXISTS _migrations (id INTEGER PRIMARY KEY);


                CREATE TABLE IF NOT EXISTS crawl_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    crawled_at TEXT,
                    total_count INTEGER,
                    urgent_count INTEGER,
                    status TEXT,
                    source TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_region ON listings(region);
                CREATE INDEX IF NOT EXISTS idx_district ON listings(district);
                CREATE INDEX IF NOT EXISTS idx_property_type ON listings(property_type);
                CREATE INDEX IF NOT EXISTS idx_is_urgent ON listings(is_urgent);
                CREATE INDEX IF NOT EXISTS idx_crawled_at ON listings(crawled_at);
                CREATE INDEX IF NOT EXISTS idx_session ON listings(crawl_session);
            """)

            cols = [r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()]
            if "naver_url" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN naver_url TEXT")
            if "price_sort_value" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN price_sort_value INTEGER")
            if "rent_sort_value" not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN rent_sort_value INTEGER")

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_sort ON listings(price_sort_value)"
            )

            self._backfill_price_sort_values(conn)

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

    def _backfill_price_sort_values(self, conn):
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

    def _get_latest_session_id(self, conn) -> Optional[str]:
        row = conn.execute(
            """
            SELECT crawl_session, MAX(crawled_at) AS last_seen
            FROM listings
            WHERE crawl_session IS NOT NULL
            GROUP BY crawl_session
            ORDER BY last_seen DESC
            LIMIT 1
            """
        ).fetchone()
        return row["crawl_session"] if row else None

    def insert_listings(self, listings: List[Dict], session_id: str):
        with self.get_connection() as conn:
            # Delete old listings from previous sessions (keep last 2 sessions)
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

            for listing in listings:
                price_value, rent_value = self._parse_price_sort_values(
                    listing.get("price"), listing.get("trade_type")
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO listings
                    (article_no, region, district, property_type, trade_type, price,
                     area, floor, building_name, description, is_urgent, tags,
                     confirmed_date, crawled_at, crawl_session, latitude, longitude, naver_url,
                     price_sort_value, rent_sort_value)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
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
                        datetime.now().isoformat(),
                        session_id,
                        listing.get("latitude"),
                        listing.get("longitude"),
                        listing.get("naver_url"),
                        price_value,
                        rent_value,
                    ),
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

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        order_map = {
            "urgent": "is_urgent DESC, crawled_at DESC",
            "recent": "crawled_at DESC",
            "price-asc": "CASE WHEN price_sort_value IS NULL THEN 1 ELSE 0 END, price_sort_value ASC, COALESCE(rent_sort_value, 0) ASC, crawled_at DESC",
            "price-desc": "CASE WHEN price_sort_value IS NULL THEN 1 ELSE 0 END, price_sort_value DESC, COALESCE(rent_sort_value, 0) DESC, crawled_at DESC",
        }
        order = order_map.get(sort_by, "is_urgent DESC, crawled_at DESC")

        with self.get_connection() as conn:
            latest_session = self._get_latest_session_id(conn)
            if latest_session:
                session_clause = "crawl_session = ?"
                session_params = [latest_session]
            else:
                session_clause = ""
                session_params = []

            combined_conditions = list(conditions)
            if session_clause:
                combined_conditions.insert(0, session_clause)

            scoped_where = (
                "WHERE " + " AND ".join(combined_conditions)
                if combined_conditions
                else ""
            )
            scoped_params = session_params + params

            total = conn.execute(
                f"SELECT COUNT(*) FROM listings {scoped_where}", scoped_params
            ).fetchone()[0]

            price_down = conn.execute(
                f"SELECT COUNT(*) FROM listings {scoped_where} {'AND' if scoped_where else 'WHERE'} tags LIKE '%가격인하%'",
                scoped_params,
            ).fetchone()[0]

            offset = (page - 1) * per_page
            rows = conn.execute(
                f"SELECT * FROM listings {scoped_where} ORDER BY {order} LIMIT ? OFFSET ?",
                scoped_params + [per_page, offset],
            ).fetchall()

            # Get type counts
            type_counts = {}
            for row in conn.execute(
                f"SELECT property_type, COUNT(*) as cnt FROM listings {scoped_where} GROUP BY property_type",
                scoped_params,
            ).fetchall():
                type_counts[row["property_type"]] = row["cnt"]

        return {
            "total": total,
            "urgent": total,  # 모든 매물이 급매
            "price_down_count": price_down,
            "type_counts": type_counts,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total + per_page - 1) // per_page),
            "listings": [dict(row) for row in rows],
        }

    def get_region_stats(self):
        with self.get_connection() as conn:
            latest_session = self._get_latest_session_id(conn)
            params = [latest_session] if latest_session else []
            where = "WHERE crawl_session = ?" if latest_session else ""
            rows = conn.execute(
                f"""
                SELECT region, district,
                       region || ' ' || district as display_name,
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
            sessions = conn.execute(
                "SELECT DISTINCT crawl_session FROM listings ORDER BY crawled_at DESC LIMIT 2"
            ).fetchall()

            if len(sessions) < 2:
                # Only one session - show all as new
                rows = conn.execute(
                    """
                    SELECT region, district,
                           region || ' ' || district as display_name,
                           COUNT(*) as current_cnt,
                           COUNT(*) as prev_cnt,
                           0 as diff,
                           SUM(CASE WHEN tags LIKE '%가격인하%' THEN 1 ELSE 0 END) as price_down_count
                    FROM listings
                    GROUP BY region, district
                    ORDER BY current_cnt DESC
                """
                ).fetchall()
            else:
                curr_session = sessions[0]["crawl_session"]
                prev_session = sessions[1]["crawl_session"]
                rows = conn.execute(
                    """
                    WITH curr AS (
                        SELECT region, district, COUNT(*) as cnt
                        FROM listings WHERE crawl_session = ?
                        GROUP BY region, district
                    ),
                    prev AS (
                        SELECT region, district, COUNT(*) as cnt
                        FROM listings WHERE crawl_session = ?
                        GROUP BY region, district
                    )
                    SELECT c.region, c.district,
                           c.region || ' ' || c.district as display_name,
                           c.cnt as current_cnt,
                           COALESCE(p.cnt, 0) as prev_cnt,
                           c.cnt - COALESCE(p.cnt, 0) as diff,
                           (SELECT SUM(CASE WHEN tags LIKE '%가격인하%' THEN 1 ELSE 0 END)
                            FROM listings l
                            WHERE l.region = c.region AND l.district = c.district
                            AND l.crawl_session = ?) as price_down_count
                    FROM curr c
                    LEFT JOIN prev p ON c.region = p.region AND c.district = p.district
                    ORDER BY diff DESC
                """,
                    (curr_session, prev_session, curr_session),
                ).fetchall()
        return [dict(row) for row in rows]

    def get_last_crawl(self):
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM crawl_history ORDER BY crawled_at DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def log_crawl(self, session_id, total_count, urgent_count, status, source):
        with self.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    datetime.now().isoformat(),
                    total_count,
                    urgent_count,
                    status,
                    source,
                ),
            )
