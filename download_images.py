"""
download_images.py — scraped.db dagi barcha mahsulot rasmlarini yuklab oladi.

Fayl nomlash:  images/{BRAND}/{SKU}-{BARCODE}-{N}.jpg
DB da tracking: downloaded_images jadvaliga yoziladi (qayta yuklamaydi).

Ishlatish:
  python download_images.py                       # hammasi
  python download_images.py --brand KOSADAKA      # bitta brand
  python download_images.py --workers 5           # parallel
  python download_images.py --limit 100           # test uchun
  python download_images.py --stats               # holat
  python download_images.py --retry-errors        # xatoliklarni qayta
  python download_images.py --db /path/scraped.db # boshqa DB
"""

import os
import re
import sys
import json
import time
import random
import logging
import sqlite3
import hashlib
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from step.anti_bot import (
    detect_captcha_or_block,
    get_proxy_dict,
    human_sleep,
    random_browser_headers,
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE            = os.path.dirname(os.path.abspath(__file__))
DB_PATH         = os.path.join(BASE, 'scraped.db')
IMG_DIR         = os.path.join(BASE, 'images')
LOG_PATH        = os.path.join(BASE, 'img_downloader.log')

MAX_WORKERS     = 4        # parallel yuklovchi thread
DELAY_MIN       = 0.3      # so'rovlar orasidagi min kutish (s)
DELAY_MAX       = 1.2      # max kutish
REQUEST_TIMEOUT = 20       # HTTP timeout
MAX_RETRIES     = 3        # bir URL uchun max urinish
MAX_IMAGES      = 20       # bitta mahsulotdan max rasm
MIN_FILE_SIZE   = 2_000    # 2 KB dan kichik — kontent emas, o'tk

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
]

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, 'session'):
        s = requests.Session()
        s.headers.update(random_browser_headers({
            'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        }))
        _thread_local.session = s
    return _thread_local.session


# ─────────────────────────────────────────────────────────────────────────────
#  FAYL NOMI YARATISH
# ─────────────────────────────────────────────────────────────────────────────

def safe(text: str, max_len: int = 60) -> str:
    """Fayl nomiga xavfsiz matn: faqat harf, raqam, tire, pastki chiziq."""
    t = re.sub(r'[^\w\-]', '_', str(text or '').strip())
    t = re.sub(r'_+', '_', t).strip('_')
    return t[:max_len]


def brand_folder(brand: str) -> Path:
    """Brand papkasini qaytaradi (mavjud bo'lmasa yaratadi)."""
    folder = Path(IMG_DIR) / safe(brand.upper(), 40)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def img_ext_from_url(url: str, content_type: str = '') -> str:
    """URLdan yoki Content-Type dan kengaytma aniqlaydi."""
    # Content-Type dan
    ct_map = {
        'image/jpeg': '.jpg', 'image/jpg': '.jpg',
        'image/png':  '.png', 'image/webp': '.webp',
        'image/gif':  '.gif', 'image/avif': '.avif',
    }
    for ct, ext in ct_map.items():
        if ct in content_type.lower():
            return ext
    # URLdan
    m = re.search(r'\.(jpg|jpeg|png|webp|gif|avif)(\?|$)', url, re.IGNORECASE)
    if m:
        return '.' + m.group(1).lower().replace('jpeg', 'jpg')
    return '.jpg'


def make_filename(brand: str, sku: str, barcode: str, idx: int,
                  url: str, content_type: str = '') -> str:
    """
    Fayl nomi: {SKU}-{BARCODE}-{N}.jpg
    Agar sku yoki barcode bo'sh bo'lsa — URLdan hash ishlatiladi.
    """
    ext = img_ext_from_url(url, content_type)
    sku_part     = safe(sku)     or hashlib.md5(url.encode()).hexdigest()[:8]
    barcode_part = safe(barcode) or 'nobc'
    return f"{sku_part}-{barcode_part}-{idx:02d}{ext}"


# ─────────────────────────────────────────────────────────────────────────────
#  RASM YUKLAB OLISH
# ─────────────────────────────────────────────────────────────────────────────

def download_image(url: str, local_path: str, retry: int = 0) -> int:
    """
    Rasmni yuklab local_path ga saqlaydi.
    Qaytaradi: fayl hajmi (bayt), xato bo'lsa 0.
    """
    session = get_session()
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True, proxies=get_proxy_dict())

        blocked, _ = detect_captcha_or_block(resp.status_code, getattr(resp, 'text', '')[:1000])
        if blocked:
            if retry < MAX_RETRIES:
                human_sleep(4, 8)
                return download_image(url, local_path, retry + 1)
            return 0

        if resp.status_code == 404:
            return 0

        if resp.status_code in (429, 503):
            wait = 30 + random.uniform(0, 15)
            log.warning(f"  ⏳ Rate limit {resp.status_code}, {wait:.0f}s kutmoqda...")
            human_sleep(wait, wait + 1)
            if retry < MAX_RETRIES:
                return download_image(url, local_path, retry + 1)
            return 0

        if resp.status_code != 200:
            log.debug(f"  HTTP {resp.status_code}: {url[:70]}")
            return 0

        content_type = resp.headers.get('Content-Type', '')
        if 'image' not in content_type and 'octet-stream' not in content_type:
            log.debug(f"  Rasm emas ({content_type}): {url[:60]}")
            return 0

        # Fayl kengaytmasini Content-Type dan aniqlashtirish
        ext = img_ext_from_url(url, content_type)
        if not local_path.endswith(ext):
            base = re.sub(r'\.[a-z]+$', '', local_path)
            local_path = base + ext

        total = 0
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
                total += len(chunk)

        if total < MIN_FILE_SIZE:
            os.remove(local_path)
            log.debug(f"  Juda kichik ({total}B): {url[:60]}")
            return 0

        return total

    except requests.exceptions.SSLError:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT,
                               stream=True, verify=False, proxies=get_proxy_dict())
            if resp.status_code == 200:
                total = 0
                with open(local_path, 'wb') as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)
                        total += len(chunk)
                return total if total >= MIN_FILE_SIZE else 0
        except Exception:
            return 0
    except requests.exceptions.ConnectionError:
        if retry < MAX_RETRIES:
            human_sleep(8 * (retry + 1), 12 * (retry + 1))
            _thread_local.session = None  # sessiyani yangilash
            return download_image(url, local_path, retry + 1)
        return 0
    except requests.exceptions.ReadTimeout:
        if retry < 1:
            human_sleep(4, 7)
            return download_image(url, local_path, retry + 1)
        return 0
    except Exception as e:
        log.debug(f"  Kutilmagan xato {url[:60]}: {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
#  DB OPERATSIYALAR
# ─────────────────────────────────────────────────────────────────────────────

def ensure_downloaded_images_table(db_path: str):
    """downloaded_images jadvali mavjudligini tekshiradi, yo'q bo'lsa yaratadi."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_images (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id    INTEGER,
            src_brand     TEXT,
            src_barcode   TEXT,
            src_sku       TEXT,
            image_url     TEXT UNIQUE,
            local_path    TEXT,
            file_size     INTEGER,
            status        TEXT DEFAULT 'ok',  -- ok | error | skipped
            downloaded_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_di_url ON downloaded_images(image_url)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_di_product ON downloaded_images(product_id)")
    conn.commit()
    conn.close()


def get_pending_products(db_path: str, brand_filter: str = None,
                         limit: int = None) -> list:
    """
    Rasmlari hali yuklanmagan mahsulotlarni qaytaradi.
    images_json bo'sh yoki NULL bo'lsa — o'tkazib yuboradi.
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row

    sql = """
        SELECT
            sp.id,
            sp.src_brand,
            sp.src_sku,
            sp.src_barcode,
            sp.title,
            sp.images_json
        FROM scraped_products sp
        WHERE sp.images_json IS NOT NULL
          AND sp.images_json != '[]'
          AND sp.images_json != ''
          AND sp.id NOT IN (
              SELECT DISTINCT product_id FROM downloaded_images
              WHERE product_id IS NOT NULL AND status = 'ok'
          )
    """
    params = []
    if brand_filter:
        sql += " AND UPPER(sp.src_brand) = UPPER(?)"
        params.append(brand_filter)

    sql += " ORDER BY sp.id ASC"

    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def is_url_downloaded(db_path: str, image_url: str) -> bool:
    """Rasm URLi allaqachon yuklab olinganini tekshiradi."""
    conn = sqlite3.connect(db_path, timeout=10)
    row = conn.execute(
        "SELECT id FROM downloaded_images WHERE image_url=? AND status='ok'",
        (image_url,)
    ).fetchone()
    conn.close()
    return row is not None


def save_image_record(db_path: str, product_id: int, brand: str,
                      sku: str, barcode: str, image_url: str,
                      local_path: str, file_size: int, status: str = 'ok'):
    """downloaded_images jadvaliga yozadi."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.execute("""
            INSERT OR REPLACE INTO downloaded_images
                (product_id, src_brand, src_sku, src_barcode,
                 image_url, local_path, file_size, status, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (product_id, brand, sku, barcode,
              image_url, local_path, file_size, status))
        conn.commit()
    except Exception as e:
        log.debug(f"DB yozish xato: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  BITTA MAHSULOT RASMLARINI YUKLAB OLISH
# ─────────────────────────────────────────────────────────────────────────────

def process_product(row, db_path: str) -> dict:
    """
    Bitta mahsulotning barcha rasmlarini yuklab oladi.
    Qaytaradi: {'product_id', 'ok', 'skip', 'error', 'total_bytes'}
    """
    product_id = row['id']
    brand      = row['src_brand'] or 'UNKNOWN'
    sku        = row['src_sku']   or ''
    barcode    = row['src_barcode'] or ''
    title      = (row['title'] or '')[:50]

    result = {'product_id': product_id, 'ok': 0, 'skip': 0,
              'error': 0, 'total_bytes': 0}

    # images_json parse
    try:
        images = json.loads(row['images_json'] or '[]')
        if not isinstance(images, list):
            images = [images]
    except Exception:
        log.debug(f"  images_json parse xato: product_id={product_id}")
        return result

    if not images:
        return result

    images = images[:MAX_IMAGES]
    folder = brand_folder(brand)

    for idx, img_url in enumerate(images, start=1):
        if not img_url or not str(img_url).startswith('http'):
            result['skip'] += 1
            continue

        img_url = str(img_url).strip()

        # Allaqachon yuklab olinganmi?
        if is_url_downloaded(db_path, img_url):
            result['skip'] += 1
            continue

        # Fayl nomi (kengaytma hali .jpg, keyin to'g'rilanadi)
        filename = make_filename(brand, sku, barcode, idx, img_url)
        local_path = str(folder / filename)

        # Yuklab olish
        human_sleep(DELAY_MIN, DELAY_MAX)
        file_size = download_image(img_url, local_path)

        if file_size > 0:
            # Kengaytma o'zgargan bo'lishi mumkin
            actual_path = local_path
            for ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.avif']:
                candidate = re.sub(r'\.[a-z]+$', ext, local_path)
                if os.path.exists(candidate):
                    actual_path = candidate
                    break

            save_image_record(db_path, product_id, brand, sku, barcode,
                              img_url, actual_path, file_size, 'ok')
            result['ok'] += 1
            result['total_bytes'] += file_size
        else:
            save_image_record(db_path, product_id, brand, sku, barcode,
                              img_url, '', 0, 'error')
            result['error'] += 1

    if result['ok'] > 0:
        log.info(
            f"  ✓ [{brand:<12}] {title:<45} | "
            f"🖼{result['ok']} ta "
            f"({result['total_bytes']//1024} KB)"
        )
    elif result['error'] > 0:
        log.warning(
            f"  ✗ [{brand:<12}] {title:<45} | "
            f"❌{result['error']} ta xato"
        )

    return result


# ─────────────────────────────────────────────────────────────────────────────
#  ASOSIY PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_downloader(db_path: str = DB_PATH, brand_filter: str = None,
                   limit: int = None, workers: int = MAX_WORKERS):

    if not os.path.exists(db_path):
        log.error(f"DB topilmadi: {db_path}")
        sys.exit(1)

    ensure_downloaded_images_table(db_path)
    Path(IMG_DIR).mkdir(parents=True, exist_ok=True)

    log.info(f"\n{'='*60}")
    log.info(f"  RASM YUKLOVCHI BOSHLANDI")
    log.info(f"  DB: {db_path}")
    log.info(f"  IMG: {IMG_DIR}")
    log.info(f"  workers={workers} | brand={brand_filter or 'hammasi'}")
    log.info(f"{'='*60}\n")

    total_ok = total_err = total_skip = total_bytes = 0
    batch_n = 0

    while True:
        batch_size = workers * 10 if not limit else limit
        products = get_pending_products(db_path, brand_filter, batch_size)

        if not products:
            log.info("Barcha mahsulotlar qayta ishlandi. Tugadi.")
            break

        log.info(f"Batch #{batch_n+1}: {len(products)} ta mahsulot...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_product, row, db_path): row
                for row in products
            }
            for f in as_completed(futures):
                try:
                    r = f.result()
                    total_ok    += r['ok']
                    total_err   += r['error']
                    total_skip  += r['skip']
                    total_bytes += r['total_bytes']
                except Exception as e:
                    log.error(f"  Thread xato: {e}")

        batch_n += 1
        log.info(
            f"\n{'─'*50}\n"
            f"  BATCH #{batch_n} | "
            f"✓{total_ok}  ✗{total_err}  ⏭{total_skip}  "
            f"💾{total_bytes//1024//1024} MB\n"
            f"{'─'*50}"
        )

        # Faqat limit bo'lsa bir marta
        if limit:
            break

        # Keyingi batch oldidan kichik pauza
        pause = random.uniform(2.0, 5.0)
        log.info(f"  {pause:.1f}s tanaffus...")
        human_sleep(pause, pause + 1)

    # Yakuniy statistika
    log.info(
        f"\n{'='*60}\n"
        f"  YUKLAB OLISH YAKUNLANDI\n"
        f"  ✓ ok     : {total_ok}\n"
        f"  ✗ error  : {total_err}\n"
        f"  ⏭ skip   : {total_skip}\n"
        f"  💾 hajm   : {total_bytes//1024//1024} MB\n"
        f"{'='*60}\n"
    )
    show_stats(db_path)


# ─────────────────────────────────────────────────────────────────────────────
#  STATISTIKA
# ─────────────────────────────────────────────────────────────────────────────

def show_stats(db_path: str = DB_PATH):
    if not os.path.exists(db_path):
        print("DB topilmadi.")
        return

    conn = sqlite3.connect(db_path)
    print(f"\n{'─'*55}")
    print("  RASM YUKLAB OLISH HOLATI")
    print(f"{'─'*55}")

    # Jami mahsulotlar
    total_prod = conn.execute(
        "SELECT COUNT(*) FROM scraped_products "
        "WHERE images_json IS NOT NULL AND images_json != '[]'"
    ).fetchone()[0]

    # Yuklab olingan mahsulotlar
    done_prod = conn.execute(
        "SELECT COUNT(DISTINCT product_id) FROM downloaded_images WHERE status='ok'"
    ).fetchone()[0]

    # Rasmlar statistikasi
    img_ok  = conn.execute(
        "SELECT COUNT(*) FROM downloaded_images WHERE status='ok'"
    ).fetchone()[0]
    img_err = conn.execute(
        "SELECT COUNT(*) FROM downloaded_images WHERE status='error'"
    ).fetchone()[0]
    total_size = conn.execute(
        "SELECT COALESCE(SUM(file_size),0) FROM downloaded_images WHERE status='ok'"
    ).fetchone()[0]

    print(f"  Mahsulotlar  : {done_prod}/{total_prod} ta bajarildi")
    print(f"  Rasmlar ✓   : {img_ok} ta")
    print(f"  Rasmlar ✗   : {img_err} ta")
    print(f"  Jami hajm   : {total_size//1024//1024} MB")
    print(f"{'─'*55}")

    # Brand bo'yicha
    print(f"  BRAND BO'YICHA:")
    rows = conn.execute("""
        SELECT src_brand, COUNT(*) as cnt,
               COALESCE(SUM(file_size),0) as sz
        FROM downloaded_images
        WHERE status='ok'
        GROUP BY src_brand
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for brand, cnt, sz in rows:
        print(f"    {(brand or 'UNKNOWN'):<15}: {cnt:>5} ta  {sz//1024//1024:>4} MB")

    print(f"{'─'*55}\n")
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]

    db_path      = DB_PATH
    brand_filter = None
    limit_n      = None
    workers_n    = MAX_WORKERS

    if '--db' in args:
        i = args.index('--db')
        db_path = args[i + 1]

    if '--brand' in args:
        i = args.index('--brand')
        brand_filter = args[i + 1]

    if '--limit' in args:
        i = args.index('--limit')
        limit_n = int(args[i + 1])

    if '--workers' in args:
        i = args.index('--workers')
        workers_n = int(args[i + 1])

    if '--stats' in args:
        show_stats(db_path)

    elif '--retry-errors' in args:
        # Xato rasmlarni o'chirib qayta urinish
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM downloaded_images WHERE status='error'")
        cnt = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
        conn.close()
        print(f"{cnt} ta xato yozuv o'chirildi, qayta yuklanmoqda...")
        run_downloader(db_path, brand_filter, limit_n, workers_n)

    else:
        run_downloader(db_path, brand_filter, limit_n, workers_n)
