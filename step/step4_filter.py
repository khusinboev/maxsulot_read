"""
STEP 4: URL filtrlash va clean_products table ga yozish

Maqsad:
  search_results dan ZIPBAITS / WORMIX / KOSADAKA mahsulotlari uchun
  yig'ilgan URL larni bir-bir tekshiradi va faqat haqiqiy mahsulot
  sahifalarini yangi `clean_products` table ga saqlaydi.

Yangi table tarkibi (keyingi loyiha uchun):
  brand, barcode, sku, product_id, product_name, url, url_domain,
  filter_score, filter_reason

Ishlatish:
  python step4_filter.py
  python step4_filter.py --min-score 40   # balini o'zgartirish
  python step4_filter.py --debug          # rad etilganlarni ham ko'rish
"""

import sqlite3
import os
import re
import sys
import logging
from urllib.parse import urlparse

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../pipeline.db')
TARGET_BRANDS = {'ZIPBAITS', 'WORMIX', 'KOSADAKA'}
MIN_SCORE     = 45      # shu va yuqori → qabul
DEBUG_MODE    = False   # True bo'lsa rad etilganlar ham logga yoziladi

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../filter.log')
logging.basicConfig(
    level   = logging.DEBUG if DEBUG_MODE else logging.INFO,
    format  = '%(asctime)s [%(levelname)s] %(message)s',
    handlers= [
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMENLAR RO'YXATLARI
# ═══════════════════════════════════════════════════════════════════════════════

# Katta marketplace domenlar (mahsulot sahifasi bo'lsa +30 ball)
TIER1_MARKETPLACES = {
    'wildberries.ru',
    'ozon.ru',
    'market.yandex.ru',
    'kaspi.kz',
    'aliexpress.com', 'aliexpress.ru',
    'amazon.com', 'amazon.de', 'amazon.co.uk',
    'sbermegamarket.ru', 'megamarket.ru',
    'goods.ru',
    'uzum.uz',
    'lamoda.ru',
    'dns-shop.ru',
    'mvideo.ru',
    'eldorado.ru',
    'citilink.ru',
    'sportmaster.ru',
    'rozetka.com.ua',
    'prom.ua',
    'olx.uz', 'olx.ua', 'olx.kz',
    'avito.ru',
    'detmir.ru',
}

# Ixtisoslashgan baliq / outdoor do'konlar (+20 ball)
TIER2_FISHING_STORES = {
    'fmagazin.ru',
    'spinningline.ru',
    'lovitut.ru',
    'rybolov.ru',
    'rybalkashop.ru',
    'trofey.uz',
    'ribolovnye.ru',
    'fishingsib.ru',
    'mir-rybalki.ru',
    'rybak.kz',
    'blesna.ru',
    'vobler.ru',
    'rsfish.ru',
    'spinningclub.ru',
    'anglers.ru',
    'rybalka.by',
    'fishclub.ru',
    'snastochka.ru',
    'nalivnuyu.ru',
    'foton.ru',           # baliq uskunalari
    'decathlon.ru', 'decathlon.ua', 'decathlon.kz',
}

# Qat'iy rad etish (blacklist) — bu domenlardan hech qachon qabul qilinmaydi
BLACKLIST_DOMAINS = {
    # ── Ijtimoiy tarmoqlar ──
    'vk.com', 'vkontakte.ru',
    'facebook.com', 'fb.com', 'instagram.com',
    'twitter.com', 'x.com',
    'tiktok.com',
    'ok.ru', 'odnoklassniki.ru',
    'pinterest.com', 'pinterest.ru',
    'reddit.com',
    'linkedin.com',
    # ── Video platformalar ──
    'youtube.com', 'youtu.be',
    'rutube.ru', 'vimeo.com',
    'epicube.cc', 'kinogo.cc', 'twitch.tv',
    # ── Maqolalar / Wikipedia ──
    'wikipedia.org', 'wikimedia.org', 'wikihow.com',
    'dzen.ru', 'zen.yandex.ru',
    # ── Ob-havo / Xarita ──
    'gismeteo.ru', 'gismeteo.ua', 'gismeteo.com',
    'weather.com', 'pogoda.ru',
    'maps.google.com', '2gis.ru', '2gis.kz', '2gis.ua',
    # ── Yangilik / Texno saytlar ──
    'itc.ua', 'ixbt.com', 'igromania.ru', 'dtf.ru',
    'habr.com', 'pikabu.ru', 'vc.ru',
    # ── Kriptovalyuta / Moliya ──
    'tonviewer.com', 'coinmarketcap.com', 'binance.com', 'blockchain.com',
    # ── Ish / Kadrlar ──
    'hh.ru', 'headhunter.ru', 'superjob.ru', 'rabota.ru', 'zarplata.ru',
    # ── Bog'liq bo'lmagan saytlar ──
    'kopirka.ru', 'elec.ru', 'gosuslugi.ru',
    'yandex.com',        # yandex bosh sahifasi (market.yandex.ru emas!)
    'auto.ru',           # mashina saytlari
    'drive2.ru',
    'eda.ru',            # oziq-ovqat
    'delivery-club.ru',
    # ── Eng mashhur news saytlar ──
    'rbc.ru', 'ria.ru', 'lenta.ru', 'gazeta.ru',
    'kommersant.ru', 'forbes.ru',
}


# ═══════════════════════════════════════════════════════════════════════════════
#  URL PATTERN QOIDALARI
# ═══════════════════════════════════════════════════════════════════════════════

# Mahsulot sahifasi belgisi (path da bo'lsa +15)
PRODUCT_URL_PATTERNS = [
    r'/product[s]?/\w',
    r'/catalog/\d{4,}',           # /catalog/184004257
    r'/catalog/\d{4,}/detail',    # /catalog/.../detail.aspx
    r'detail\.aspx',
    r'/card/[\w-]',               # yandex.market /card/...
    r'/item[s]?/\w',
    r'/tovar[y]?/[\w-]',
    r'/p/\d{4,}',
    r'/goods/\d{4,}',
    r'/offer/[\w-]',
    r'/buy/[\w-]',
    r'/\d{8,}/?$',                # oxirida 8+ raqam (ozon, wb ID)
    r'/[\w-]{15,}/\d{8,}',       # slug + uzoq ID (yandex.market)
]

# Mahsulot EMAS sahifalar (home, category, search) → -25
NON_PRODUCT_PATTERNS = [
    r'^/?$',                      # bosh sahifa
    r'^/catalog/?$',              # faqat /catalog/
    r'^/catalog/[a-z_-]+/?$',    # /catalog/spinning/ — kategoriya
    r'^/category',
    r'^/categories',
    r'^/search',
    r'^/tag[s]?/',
    r'^/page/\d+/?$',
    r'^/brand[s]?/?$',
    r'^/news/',
    r'^/article[s]?/',
    r'^/blog/',
    r'^/forum',
    r'^/review[s]?/',
    r'^/help/',
    r'^/faq',
]

# Baliq / lure kalit so'zlari (URL da bo'lsa +10)
FISHING_KW_RE = re.compile(
    r'(vobler|wobbler|blesn[ai]|spinner|jig+|lure|fishing|'
    r'rybalk|rybolov|snast|priman|spinning|trolling|feeder|'
    r'rattlin|popper|minnow|crankbait|swimbait|pike|perch|'
    r'okun|shchuk|karp|sudak)',
    re.IGNORECASE
)

# Brand URL ko'rinishlari
BRAND_HINTS: dict[str, list[str]] = {
    'ZIPBAITS': ['zipbait', 'zip-bait', 'zip_bait'],
    'WORMIX':   ['wormix'],
    'KOSADAKA': ['kosadak'],
}

# Mahsulot nomi tokenlarida istisno qilinuvchi umumiy so'zlar
TOKEN_STOPWORDS = {
    'xs', 'sp', 'ss', 'mm', 'cm', 'gr', 'kg', 'mg', 'oz', 'lb',
    'to', 'in', 'at', 'the', 'for', 'and', 'of', 'by', 'with',
    'plav', 'top', 'un', 'sup', 'sub', 'pre', 'pro',
    'new', 'old', 'big', 'max', 'mini', 'plus', 'lite',
}


# ═══════════════════════════════════════════════════════════════════════════════
#  YORDAMCHI FUNKSIYALAR
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_domain(netloc: str) -> str:
    """www.wildberries.ru → wildberries.ru"""
    return re.sub(r'^www\.', '', netloc.lower().strip())


def domain_in_set(domain: str, domain_set: set) -> bool:
    """Domen yoki uning subdomain to'plamda bormi?"""
    for d in domain_set:
        if domain == d or domain.endswith('.' + d):
            return True
    return False


def latin_tokens(text: str) -> set:
    """Matndan lotin + raqam tokenlarni ajratadi (2+ belgi)."""
    return set(re.findall(r'[a-z0-9]{2,}', text.lower()))


def product_name_tokens(product_name: str) -> set:
    """Mahsulot nomidan muhim lotin tokenlarni oladi."""
    tokens = latin_tokens(product_name)
    return tokens - TOKEN_STOPWORDS


# ═══════════════════════════════════════════════════════════════════════════════
#  ASOSIY BAHOLASH FUNKSIYASI
# ═══════════════════════════════════════════════════════════════════════════════

def score_url(url: str, brand: str, product_name: str) -> tuple[int, list[str]]:
    """
    URL ga ball beradi. Yuqori ball → haqiqiy mahsulot sahifasi.

    Ball tizimi:
      +30  — tier1 marketplace
      +20  — tier2 fishing store
      +15  — mahsulot URL pattern
      +20  — brand nomi URL da
      +8×N — mahsulot tokenlaridan N tasi URL da (max +24)
      +10  — baliq ovlash kalit so'zi URL da
      +5   — URL chuqurligi ≥ 3
      -25  — homepage / kategoriya URL
      -10  — URL chuqurligi ≤ 1
      -100 — blacklist

    Qaytaradi: (score: int, reasons: list[str])
    """
    score: int      = 0
    reasons: list   = []

    # ── URL parse ─────────────────────────────────────────────────────────────
    try:
        if not url.startswith('http'):
            url = 'https://' + url
        parsed = urlparse(url)
    except Exception:
        return -999, ['invalid_url']

    domain   = normalize_domain(parsed.netloc)
    path     = parsed.path.lower()
    full_url = url.lower()

    # ── 1. Blacklist (darhol rad) ─────────────────────────────────────────────
    if domain_in_set(domain, BLACKLIST_DOMAINS):
        return -100, ['blacklisted_domain']

    # ── 2. Domen darajasi ────────────────────────────────────────────────────
    if domain_in_set(domain, TIER1_MARKETPLACES):
        score += 30
        reasons.append('tier1_marketplace')
    elif domain_in_set(domain, TIER2_FISHING_STORES):
        score += 20
        reasons.append('tier2_fishing_store')
    else:
        reasons.append('unknown_domain')

    # ── 3. URL pattern tekshiruvi ─────────────────────────────────────────────
    is_product = any(re.search(p, path, re.IGNORECASE)
                     for p in PRODUCT_URL_PATTERNS)
    is_non_prd = any(re.match(p, path, re.IGNORECASE)
                     for p in NON_PRODUCT_PATTERNS)

    if is_product:
        score += 15
        reasons.append('product_url_pattern')

    if is_non_prd:
        score -= 25
        reasons.append('non_product_url_pattern')

    # ── 4. Brand nomi URL da bormi? ───────────────────────────────────────────
    brand_hints = BRAND_HINTS.get(brand, [brand.lower()])
    if any(hint in full_url for hint in brand_hints):
        score += 20
        reasons.append('brand_in_url')

    # ── 5. Mahsulot nomi tokenlaridan URL da qancha? ─────────────────────────
    pn_tokens  = product_name_tokens(product_name)
    url_tokens = latin_tokens(full_url)
    matched    = pn_tokens & url_tokens
    # Brand tokenlarini qayta hisoblamaslik
    matched -= set(''.join(brand_hints).replace('-', '').replace('_', ''))
    matched -= {h.replace('-', '').replace('_', '') for h in brand_hints}
    if matched:
        bonus = min(len(matched) * 8, 24)
        score += bonus
        reasons.append(f'product_tokens({list(matched)[:4]})+{bonus}')

    # ── 6. Baliq ovlash kalit so'zi ───────────────────────────────────────────
    if FISHING_KW_RE.search(full_url):
        score += 10
        reasons.append('fishing_keyword_in_url')

    # ── 7. URL chuqurligi ─────────────────────────────────────────────────────
    depth = len([seg for seg in path.split('/') if seg])
    if depth >= 3:
        score += 5
        reasons.append(f'depth={depth}')
    elif depth <= 1:
        score -= 10
        reasons.append(f'shallow_depth={depth}')

    return score, reasons


# ═══════════════════════════════════════════════════════════════════════════════
#  DATABASE FUNKSIYALARI
# ═══════════════════════════════════════════════════════════════════════════════

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_clean_table(conn: sqlite3.Connection):
    """Tozalangan mahsulotlar uchun yangi table."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS clean_products (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,

        -- Keyingi loyiha uchun asosiy maydonlar
        brand            TEXT    NOT NULL,
        barcode          TEXT,
        sku              TEXT,
        product_id       TEXT,
        product_name     TEXT,
        url              TEXT    NOT NULL,

        -- Filter meta-ma'lumot
        url_domain       TEXT,
        filter_score     INTEGER,
        filter_reason    TEXT,
        original_task_id INTEGER,

        created_at       TEXT    DEFAULT (datetime('now')),

        -- Bir mahsulot uchun bir xil URL ikki marta kiritilmasin
        UNIQUE(barcode, url)
    );

    CREATE INDEX IF NOT EXISTS idx_clean_brand   ON clean_products(brand);
    CREATE INDEX IF NOT EXISTS idx_clean_barcode ON clean_products(barcode);
    CREATE INDEX IF NOT EXISTS idx_clean_url     ON clean_products(url);
    CREATE INDEX IF NOT EXISTS idx_clean_score   ON clean_products(filter_score);
    """)
    conn.commit()
    log.info("[DB] clean_products table tayyor")


def load_raw_data(conn: sqlite3.Connection) -> list:
    """TARGET_BRANDS uchun barcha search_results ni yuklaydi."""
    placeholders = ','.join('?' * len(TARGET_BRANDS))
    cur = conn.execute(f"""
        SELECT
            t.id          AS task_id,
            t.brand,
            t.barcode,
            t.sku,
            t.product_id,
            t.text1       AS product_name,
            r.url,
            r.title,
            r.snippet,
            r.position
        FROM search_results r
        JOIN search_tasks   t ON r.task_id = t.id
        WHERE t.brand IN ({placeholders})
          AND t.status = 'done'
          AND r.url IS NOT NULL
          AND r.url != ''
        ORDER BY t.brand, t.product_id, r.position
    """, list(TARGET_BRANDS))
    return cur.fetchall()


# ═══════════════════════════════════════════════════════════════════════════════
#  ASOSIY FUNKSIYA
# ═══════════════════════════════════════════════════════════════════════════════

def run_filter(min_score: int = MIN_SCORE, debug: bool = DEBUG_MODE):
    conn = get_conn()
    create_clean_table(conn)

    rows = load_raw_data(conn)
    brands_found = set(row['brand'] for row in rows)
    log.info(
        f"\n{'='*55}\n"
        f"  Filtrlash boshlanmoqda\n"
        f"  Brendlar : {', '.join(sorted(brands_found))}\n"
        f"  Jami URL : {len(rows)}\n"
        f"  Min ball : {min_score}\n"
        f"{'='*55}"
    )

    accepted        = 0
    rejected        = 0
    already_exist   = 0

    # Brand bo'yicha statistika
    brand_stats: dict[str, dict] = {
        b: {'total': 0, 'accepted': 0, 'rejected': 0}
        for b in brands_found
    }

    for row in rows:
        url          = row['url']
        brand        = row['brand']
        product_name = row['product_name'] or ''
        barcode      = row['barcode'] or ''
        sku          = row['sku'] or ''
        product_id   = row['product_id'] or ''
        task_id      = row['task_id']

        brand_stats[brand]['total'] += 1

        score, reasons = score_url(url, brand, product_name)
        reason_str     = ' | '.join(reasons)

        try:
            parsed_domain = normalize_domain(urlparse(url).netloc)
        except Exception:
            parsed_domain = ''

        if score >= min_score:
            try:
                cur = conn.execute("""
                    INSERT OR IGNORE INTO clean_products
                        (brand, barcode, sku, product_id, product_name,
                         url, url_domain, filter_score, filter_reason,
                         original_task_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (brand, barcode, sku, product_id, product_name,
                      url, parsed_domain, score, reason_str, task_id))
                conn.commit()

                if cur.rowcount > 0:
                    accepted += 1
                    brand_stats[brand]['accepted'] += 1
                    log.info(f"  ✓ [{score:3d}] {brand:<10} {url[:80]}")
                else:
                    already_exist += 1

            except Exception as e:
                log.warning(f"  ! INSERT xato: {e} | {url[:60]}")
        else:
            rejected += 1
            brand_stats[brand]['rejected'] += 1
            if debug:
                log.debug(f"  ✗ [{score:3d}] {brand:<10} {url[:70]}  ← {reason_str}")

    # Yakuniy hisobot
    total = len(rows)
    log.info(
        f"\n{'='*55}\n"
        f"  FILTER YAKUNLANDI\n"
        f"  Jami URL           : {total}\n"
        f"  Qabul qilindi      : {accepted}  ({accepted/max(total,1)*100:.1f}%)\n"
        f"  Rad etildi         : {rejected}  ({rejected/max(total,1)*100:.1f}%)\n"
        f"  Dublikat (o'tildi) : {already_exist}\n"
        f"{'─'*55}"
    )
    for brand, s in sorted(brand_stats.items()):
        log.info(
            f"  {brand:<12} | jami:{s['total']:>5} | "
            f"✓{s['accepted']:>4} | ✗{s['rejected']:>4}"
        )
    log.info(f"{'='*55}\n")
    conn.close()


def show_stats(min_score: int = MIN_SCORE):
    """clean_products tableda qancha ma'lumot borligini ko'rish."""
    db = DB_PATH
    if not os.path.exists(db):
        print("pipeline.db topilmadi.")
        return
    conn = sqlite3.connect(db)
    cur  = conn.execute("""
        SELECT brand, COUNT(*) as urls, COUNT(DISTINCT barcode) as products
        FROM clean_products
        GROUP BY brand
    """)
    rows = cur.fetchall()
    if not rows:
        print("clean_products bo'sh (avval: python step4_filter.py)")
        return
    print(f"\n{'─'*45}")
    print(f"  clean_products holati")
    print(f"{'─'*45}")
    total_urls = 0
    total_prod = 0
    for brand, urls, prods in rows:
        print(f"  {brand:<12} | {prods:>4} mahsulot | {urls:>5} URL")
        total_urls += urls
        total_prod += prods
    print(f"{'─'*45}")
    print(f"  JAMI       | {total_prod:>4} mahsulot | {total_urls:>5} URL")
    print(f"{'─'*45}\n")
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    args = sys.argv[1:]

    min_score = MIN_SCORE
    debug     = DEBUG_MODE

    # Argumentlarni parse
    if '--min-score' in args:
        idx = args.index('--min-score')
        try:
            min_score = int(args[idx + 1])
            print(f"Min score: {min_score}")
        except (IndexError, ValueError):
            print("--min-score dan keyin raqam kiriting")
            sys.exit(1)

    if '--debug' in args:
        debug = True
        logging.getLogger().setLevel(logging.DEBUG)

    if '--stats' in args:
        show_stats()
    else:
        run_filter(min_score=min_score, debug=debug)
        show_stats()
