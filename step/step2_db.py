"""
STEP 2: SQLite bazasini sozlash
Jadvallar:
  - search_tasks : har bir qidiruv vazifasi (text1/text2, holat)
  - search_results: topilgan URL lar
"""

import sqlite3
import json
import os


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../pipeline.db')


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # parallel read/write
    conn.execute("PRAGMA synchronous=NORMAL") # tezlik/xavfsizlik balansi
    return conn


def init_db():
    conn = get_conn()
    cur  = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS search_tasks (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        brand       TEXT    NOT NULL,
        product_id  TEXT,
        sku         TEXT,
        barcode     TEXT,
        text1       TEXT    NOT NULL,
        text2       TEXT,
        query_used  TEXT,           -- qaysi text ishlatildi (text1/text2)
        status      TEXT    NOT NULL DEFAULT 'pending',
                                    -- pending | done | error | no_results
        attempt     INTEGER DEFAULT 0,
        error_msg   TEXT,
        created_at  TEXT    DEFAULT (datetime('now')),
        updated_at  TEXT    DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS search_results (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id     INTEGER NOT NULL REFERENCES search_tasks(id),
        brand       TEXT,
        product_id  TEXT,
        url         TEXT    NOT NULL,
        title       TEXT,
        snippet     TEXT,
        position    INTEGER,        -- 1-dan boshlab (birinchi natija = 1)
        created_at  TEXT    DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_tasks_status  ON search_tasks(status);
    CREATE INDEX IF NOT EXISTS idx_tasks_brand   ON search_tasks(brand);
    CREATE INDEX IF NOT EXISTS idx_results_task  ON search_results(task_id);
    CREATE INDEX IF NOT EXISTS idx_results_url   ON search_results(url);
    """)

    conn.commit()
    conn.close()
    print(f"[DB] Baza tayyor: {DB_PATH}")


def load_queries_to_db(queries_json_path: str):
    """search_queries.json dan DB ga vazifalarni yuklaydi (dublikat bo'lmaydi)."""
    with open(queries_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn  = get_conn()
    cur   = conn.cursor()
    added = 0
    skip  = 0

    for brand, products in data.items():
        for p in products:
            # Dublikat tekshirish (barcode + brand)
            cur.execute(
                "SELECT id FROM search_tasks WHERE brand=? AND barcode=? LIMIT 1",
                (brand, p['barcode'])
            )
            if cur.fetchone():
                skip += 1
                continue

            cur.execute("""
                INSERT INTO search_tasks (brand, product_id, sku, barcode, text1, text2)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (brand, p['id'], p['sku'], p['barcode'], p['text1'], p['text2']))
            added += 1

    conn.commit()
    conn.close()
    print(f"[DB] Yuklandi: {added} yangi, {skip} dublikat o'tkazildi")


if __name__ == '__main__':
    BASE = os.path.dirname(os.path.abspath(__file__))
    init_db()
    load_queries_to_db(os.path.join(BASE, '../search_queries.json'))
