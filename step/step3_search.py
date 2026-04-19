"""
STEP 3: Asosiy qidiruv pipeline
- DuckDuckGo orqali qidiradi (Tor opsional)
- text1 → natija yo'q → text2 fallback
- Rate limit / ban → kutish + retry
- Checkpoint: to'xtagan joydan davom etadi
"""

import time
import random
import logging
import os
import sqlite3
from datetime import datetime, UTC
from ddgs import DDGS
from anti_bot import get_next_proxy_url, human_sleep

# ── Logging ─────────────────────────────────────────────────────────────────
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../pipeline.log')
logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s [%(levelname)s] %(message)s',
    handlers = [
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
DB_PATH         = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../pipeline.db')
MAX_RESULTS     = 10        # birinchi page ~ 10 natija
BATCH_SIZE      = 40        # nechta so'rovdan keyin uzun tanaffus
DELAY_MIN       = 3.0       # so'rovlar orasidagi min kutish (soniya)
DELAY_MAX       = 7.0       # max kutish
BATCH_PAUSE_MIN = 45        # batch tugaganda min pauza
BATCH_PAUSE_MAX = 90        # max pauza
RATE_LIMIT_WAIT = 120       # 429/ban uchun kutish
MAX_RETRIES     = 3         # bir vazifa uchun max urinish

# ── Marketplace domenlar (filtrlash uchun, ixtiyoriy) ────────────────────────
MARKETPLACE_HINTS = [
    'wildberries.ru', 'ozon.ru', 'kaspi.kz', 'lamoda.ru',
    'sportmaster.ru', 'aliexpress.com', 'amazon.com',
    'rozetka.com.ua', 'dns-shop.ru', 'mvideo.ru', 'eldorado.ru',
    'uzum.uz', 'olx.uz', 'trofey.uz'
]


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def mark_task(conn, task_id: int, status: str, query_used: str = None,
              error_msg: str = None, attempt: int = None):
    params = [status, datetime.now(UTC).isoformat()]
    sql    = "UPDATE search_tasks SET status=?, updated_at=?"
    if query_used is not None:
        sql    += ", query_used=?"
        params.append(query_used)
    if error_msg is not None:
        sql    += ", error_msg=?"
        params.append(error_msg[:500])
    if attempt is not None:
        sql    += ", attempt=?"
        params.append(attempt)
    sql += " WHERE id=?"
    params.append(task_id)
    conn.execute(sql, params)
    conn.commit()


def save_results(conn, task_id: int, brand: str, product_id: str, results: list):
    for pos, r in enumerate(results, start=1):
        url     = r.get('href', '') or r.get('url', '')
        title   = r.get('title', '')
        snippet = r.get('body', '') or r.get('snippet', '')
        if not url:
            continue
        conn.execute("""
            INSERT OR IGNORE INTO search_results
                (task_id, brand, product_id, url, title, snippet, position)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (task_id, brand, product_id, url, title[:300], snippet[:500], pos))
    conn.commit()


def ddg_search(query: str, max_results: int = MAX_RESULTS) -> list:
    """
    DuckDuckGo qidiruvini bajaradi.
    RateLimitException yoki boshqa xatoda Exception raise qiladi.
    """
    proxy_url = get_next_proxy_url()
    kwargs = {'proxy': proxy_url} if proxy_url else {}
    with DDGS(**kwargs) as ddgs:
        results = list(ddgs.text(
            query,
            max_results = max_results,
            safesearch  = 'off',
            region      = 'ru-ru'     # CIS marketplacelari uchun
        ))
    return results


def get_pending_tasks(conn, limit: int = 500) -> list:
    cur = conn.execute("""
        SELECT * FROM search_tasks
        WHERE status IN ('pending', 'error')
          AND attempt < ?
        ORDER BY id ASC
        LIMIT ?
    """, (MAX_RETRIES, limit))
    return cur.fetchall()


def stats(conn) -> dict:
    cur = conn.execute("""
        SELECT status, COUNT(*) as cnt
        FROM search_tasks
        GROUP BY status
    """)
    return {row['status']: row['cnt'] for row in cur.fetchall()}


# ── Asosiy pipeline ──────────────────────────────────────────────────────────
def run_pipeline():
    conn = get_conn()

    # Joriy holat
    s = stats(conn)
    total = sum(s.values())
    log.info(f"Pipeline boshlandi. Jami: {total} | "
             f"pending: {s.get('pending',0)} | "
             f"done: {s.get('done',0)} | "
             f"error: {s.get('error',0)} | "
             f"no_results: {s.get('no_results',0)}")

    processed    = 0
    batch_count  = 0
    consecutive_errors = 0

    while True:
        tasks = get_pending_tasks(conn, limit=BATCH_SIZE)
        if not tasks:
            log.info("Barcha vazifalar bajarildi. Pipeline tugadi.")
            break

        for task in tasks:
            task_id    = task['id']
            brand      = task['brand']
            product_id = task['product_id']
            text1      = task['text1']
            text2      = task['text2']
            attempt    = task['attempt'] + 1

            success    = False
            query_used = None

            # text1 va text2 ro'yxati: None larni olib tashla
            queries = [('text1', text1)]
            if text2 and text2.strip() != text1.strip():
                queries.append(('text2', text2))

            for q_label, q_text in queries:
                try:
                    log.info(f"[{brand}] ID:{product_id} | {q_label}: {q_text[:60]}")
                    results = ddg_search(q_text)

                    if results:
                        save_results(conn, task_id, brand, product_id, results)
                        mark_task(conn, task_id, 'done',
                                  query_used=q_label, attempt=attempt)
                        log.info(f"  ✓ {len(results)} natija topildi ({q_label})")
                        query_used  = q_label
                        success     = True
                        consecutive_errors = 0
                        break   # text2 ga o'tmaymiz
                    else:
                        log.info(f"  – Natija yo'q ({q_label})")

                except Exception as e:
                    err_str = str(e).lower()
                    log.warning(f"  ✗ Xato ({q_label}): {e}")

                    # Rate limit / ban aniqlash
                    if any(kw in err_str for kw in
                           ['ratelimit', '429', 'too many', 'blocked', 'captcha', 'forbidden']):
                        consecutive_errors += 1
                        wait = RATE_LIMIT_WAIT * consecutive_errors
                        log.warning(f"  ⚠ Rate limit! {wait}s kutilmoqda...")
                        human_sleep(wait, wait + 1)
                        # Bu vazifani error deb belgilab keyingisiga o'tamiz
                        mark_task(conn, task_id, 'error',
                                  error_msg=str(e), attempt=attempt)
                        success = True  # loop dan chiqish uchun (boshqa query sinamas)
                        break
                    else:
                        # Boshqa xato — keyingi query ni sinab ko'r
                        continue

            # Agar barcha querylar natijasiz bo'lsa
            if not success and query_used is None:
                mark_task(conn, task_id, 'no_results', attempt=attempt,
                          error_msg='All queries returned empty')
                log.info(f"  – Hech natija yo'q, no_results deb belgilandi")

            processed   += 1
            batch_count += 1

            # So'rovlar orasidagi randomized delay
            human_sleep(DELAY_MIN, DELAY_MAX)

            # Batch tugadi → uzoqroq pauza
            if batch_count >= BATCH_SIZE:
                pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                s_now = stats(conn)
                log.info(
                    f"\n{'='*55}\n"
                    f"  BATCH TUGADI | done:{s_now.get('done',0)} | "
                    f"pending:{s_now.get('pending',0)} | "
                    f"error:{s_now.get('error',0)}\n"
                    f"  {pause:.0f}s tanaffus...\n"
                    f"{'='*55}\n"
                )
                human_sleep(pause, pause + 1)
                batch_count = 0

        # Batch da task qolmadi — davom et yoki tugat
        s_now = stats(conn)
        remaining = s_now.get('pending', 0) + s_now.get('error', 0)
        if remaining == 0:
            break

    # Yakuniy statistika
    final = stats(conn)
    log.info(
        f"\n{'='*55}\n"
        f"  PIPELINE YAKUNLANDI\n"
        f"  done:       {final.get('done',0)}\n"
        f"  no_results: {final.get('no_results',0)}\n"
        f"  error:      {final.get('error',0)}\n"
        f"{'='*55}\n"
    )
    conn.close()


if __name__ == '__main__':
    run_pipeline()
