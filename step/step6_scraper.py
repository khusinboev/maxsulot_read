"""
STEP 6: clean_products dan URL larni o'qib, har bir saytdan mahsulot
ma'lumotlarini yig'adi va yangi DB ga saqlaydi, rasmlarini yuklab oladi.

Ishlatish:
  python step6_scraper.py                    # to'liq scraping
  python step6_scraper.py --domain ozon.ru   # bitta domen
  python step6_scraper.py --limit 100        # faqat 100 URL
  python step6_scraper.py --workers 3        # parallel ishchi
  python step6_scraper.py --no-images        # rasmsiz
  python step6_scraper.py --stats            # holat
  python step6_scraper.py --retry-errors     # xatoliklarni qayta

Natija:
  scraped.db               — mahsulot ma'lumotlari
  images/{BRAND}/{SKU}-{BARCODE}-{N}.jpg — mahsulot rasmlari
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
import mimetypes
import threading
import traceback
from datetime import datetime, UTC
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from parser_utils import canonical_number_key

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE          = os.path.dirname(os.path.abspath(__file__))
SRC_DB        = os.path.join(BASE, '../pipeline.db')
DST_DB        = os.path.join(BASE, '../scraped.db')
IMG_DIR       = os.path.join(BASE, 'images')
LOG_PATH      = os.path.join(BASE, '../scraper.log')

MAX_WORKERS   = 2          # parallel threadlar
DELAY_MIN     = 2.0        # so'rovlar orasidagi min kutish
DELAY_MAX     = 5.0        # max kutish
REQUEST_TIMEOUT = 20       # HTTP timeout
MAX_RETRIES   = 3          # bir URL uchun max urinish
RETRY_DELAY   = 30         # retry dan oldin kutish
MAX_IMAGES    = 10         # bitta mahsulotdan max rasm
DOWNLOAD_IMAGES = False        # rasmlarni yuklab olish

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  HTTP SESSION — BOT DETECTION AYLANIB O'TISH
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    # Chrome Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    # Firefox
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Safari Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
    # Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
]

ACCEPT_LANGS = [
    'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'ru,en-US;q=0.9,en;q=0.8',
    'ru-RU,ru;q=0.8,en;q=0.6',
]

_thread_local = threading.local()


def make_session() -> requests.Session:
    """Thread-local session yaratadi."""
    s = requests.Session()
    ua = random.choice(USER_AGENTS)
    s.headers.update({
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': random.choice(ACCEPT_LANGS),
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    })
    s.max_redirects = 5
    return s


def get_session() -> requests.Session:
    """Thread-local session qaytaradi."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = make_session()
    return _thread_local.session


def fetch_url(url: str, referer: str = None, retry: int = 0) -> requests.Response | None:
    """URL ni yuklab oladi, xato bo'lsa retry qiladi."""
    session = get_session()
    headers = {}
    if referer:
        headers['Referer'] = referer

    # Domenga qarab qo'shimcha headerlar
    parsed = urlparse(url)
    domain = re.sub(r'^www\.', '', parsed.netloc.lower())
    origin = f"{parsed.scheme}://{parsed.netloc}"
    headers['Origin'] = origin

    # Wildberries, Ozon ga maxsus header
    if 'wildberries' in domain:
        headers['x-client-name'] = 'site'
    elif 'ozon' in domain:
        headers['x-o3-app-name'] = 'ozon'

    try:
        resp = session.get(
            url, headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True
        )
        return resp
    except requests.exceptions.TooManyRedirects:
        log.warning(f"  ⚠ Redirect loop: {url[:70]}")
        return None
    except requests.exceptions.SSLError:
        log.warning(f"  ⚠ SSL xato, verify=False bilan qayta: {url[:70]}")
        try:
            return session.get(url, headers=headers, timeout=REQUEST_TIMEOUT,
                               verify=False, allow_redirects=True)
        except Exception:
            return None
    except requests.exceptions.ConnectionError as e:
        if retry < MAX_RETRIES:
            wait = RETRY_DELAY * (retry + 1)
            log.warning(f"  ⚠ Connection error, {wait}s dan keyin retry #{retry+1}: {url[:60]}")
            time.sleep(wait)
            _thread_local.session = make_session()  # yangi session
            return fetch_url(url, referer, retry + 1)
        log.error(f"  ✗ Connection xato (max retry): {url[:60]}: {e}")
        return None
    except requests.exceptions.ReadTimeout:
        if retry < 1:
            time.sleep(10)
            return fetch_url(url, referer, retry + 1)
        log.warning(f"  ⚠ Timeout: {url[:60]}")
        return None
    except Exception as e:
        log.error(f"  ✗ Kutilmagan xato: {url[:60]}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  MA'LUMOT TUZILMASI
# ─────────────────────────────────────────────────────────────────────────────

class ProductData:
    def __init__(self):
        self.title: str = ''
        self.brand: str = ''
        self.sku: str = ''
        self.barcode: str = ''
        self.price: str = ''
        self.old_price: str = ''
        self.currency: str = 'RUB'
        self.description: str = ''
        self.rating: str = ''
        self.reviews_count: str = ''
        self.stock_status: str = ''
        self.category: str = ''
        self.breadcrumbs: str = ''
        self.images: list = []        # URL lar ro'yxati
        self.specs: dict = {}         # texnik xususiyatlar
        self.extra: dict = {}         # qo'shimcha ma'lumotlar
        self.source_url: str = ''
        self.domain: str = ''
        self.parse_method: str = ''   # qaysi parser ishlatildi

    def to_dict(self) -> dict:
        return {
            'title':          self.title,
            'brand':          self.brand,
            'sku':            self.sku,
            'barcode':        self.barcode,
            'price':          self.price,
            'old_price':      self.old_price,
            'currency':       self.currency,
            'description':    self.description,
            'rating':         self.rating,
            'reviews_count':  self.reviews_count,
            'stock_status':   self.stock_status,
            'category':       self.category,
            'breadcrumbs':    self.breadcrumbs,
            'images_json':    json.dumps(self.images, ensure_ascii=False),
            'specs_json':     json.dumps(self.specs, ensure_ascii=False),
            'extra_json':     json.dumps(self.extra, ensure_ascii=False),
            'source_url':     self.source_url,
            'domain':         self.domain,
            'parse_method':   self.parse_method,
        }


# ─────────────────────────────────────────────────────────────────────────────
#  YORDAMCHI FUNKSIYALAR
# ─────────────────────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    if not text:
        return ''
    return re.sub(r'\s+', ' ', text.strip())


def extract_price(text: str) -> str:
    """Matndan narxni barqaror chiqaradi: '1 299 ₽' -> '1299'."""
    if not text:
        return ''

    compact = text.replace('\xa0', ' ').strip()
    # 1 299 / 1299.50 / 1,299.50 kabi birinchi mantiqiy blokni olamiz.
    m = re.search(r'\d[\d\s.,]{0,20}', compact)
    if not m:
        return ''

    chunk = m.group(0).replace(' ', '')
    # Agar vergul va nuqta ikkalasi bo'lsa, oxirgisini decimal deb qabul qilamiz.
    if ',' in chunk and '.' in chunk:
        if chunk.rfind(',') > chunk.rfind('.'):
            chunk = chunk.replace('.', '').replace(',', '.')
        else:
            chunk = chunk.replace(',', '')
    else:
        chunk = chunk.replace(',', '.')

    parts = chunk.split('.')
    if len(parts) > 2:
        chunk = ''.join(parts[:-1]) + '.' + parts[-1]

    return re.sub(r'[^\d.]', '', chunk)


def build_soup(resp: requests.Response) -> BeautifulSoup:
    """HTML ni robust decode qilib parserga uzatadi."""
    html_text = ''
    encodings = [resp.encoding, resp.apparent_encoding, 'utf-8', 'cp1251']

    for enc in encodings:
        if not enc:
            continue
        try:
            html_text = resp.content.decode(enc, errors='replace')
            if html_text:
                break
        except Exception:
            continue

    if not html_text:
        html_text = resp.text or ''

    return BeautifulSoup(html_text, 'lxml')


def abs_url(url: str, base_url: str) -> str:
    """Nisbiy URL ni absolyut qiladi."""
    if not url:
        return ''
    if url.startswith('//'):
        return 'https:' + url
    if url.startswith('http'):
        return url
    return urljoin(base_url, url)


def get_meta(soup: BeautifulSoup, name: str = None, prop: str = None,
             itemprop: str = None) -> str:
    """meta teglaridan kontent oladi."""
    if name:
        tag = soup.find('meta', attrs={'name': name})
    elif prop:
        tag = soup.find('meta', attrs={'property': prop})
    elif itemprop:
        tag = soup.find('meta', attrs={'itemprop': itemprop})
    else:
        return ''
    if tag:
        return clean_text(tag.get('content', ''))
    return ''


def extract_json_ld(soup: BeautifulSoup) -> list:
    """JSON-LD ma'lumotlarini chiqaradi."""
    results = []
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            text = script.get_text(strip=True)
            data = json.loads(text)
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except Exception:
            pass
    return results


def find_product_schema(json_ld_list: list) -> dict:
    """JSON-LD dan Product schemani topadi."""
    for item in json_ld_list:
        t = item.get('@type', '')
        if isinstance(t, list):
            types = [x.lower() for x in t]
        else:
            types = [t.lower()]
        if 'product' in types:
            return item
        if isinstance(item, dict) and '@graph' in item:
            for node in item['@graph']:
                nt = node.get('@type', '').lower()
                if 'product' in nt:
                    return node
    return {}


def parse_open_graph(soup: BeautifulSoup, pd: ProductData):
    """Open Graph meta ma'lumotlarini o'qiydi."""
    pd.title     = pd.title or get_meta(soup, prop='og:title')
    pd.description = pd.description or get_meta(soup, prop='og:description')
    # og:image
    og_img = get_meta(soup, prop='og:image')
    if og_img and og_img not in pd.images:
        pd.images.append(og_img)


def parse_json_ld(soup: BeautifulSoup, pd: ProductData, base_url: str):
    """JSON-LD dan mahsulot ma'lumotlarini oladi."""
    json_ld = extract_json_ld(soup)
    product = find_product_schema(json_ld)
    if not product:
        return

    pd.title  = pd.title  or clean_text(product.get('name', ''))
    pd.brand  = pd.brand  or clean_text(
        product.get('brand', {}).get('name', '') if isinstance(product.get('brand'), dict)
        else str(product.get('brand', ''))
    )
    pd.sku    = pd.sku    or clean_text(product.get('sku', ''))
    pd.barcode = pd.barcode or clean_text(product.get('gtin13', '') or product.get('gtin', '') or product.get('mpn', ''))
    pd.description = pd.description or clean_text(product.get('description', ''))
    pd.category = pd.category or clean_text(product.get('category', ''))

    # Rasm
    img = product.get('image')
    if isinstance(img, str) and img:
        u = abs_url(img, base_url)
        if u not in pd.images:
            pd.images.append(u)
    elif isinstance(img, list):
        for i in img:
            u = abs_url(i if isinstance(i, str) else i.get('url', ''), base_url)
            if u and u not in pd.images:
                pd.images.append(u)

    # Narx
    offers = product.get('offers', {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        pd.price    = pd.price    or extract_price(str(offers.get('price', '')))
        pd.currency = pd.currency or offers.get('priceCurrency', 'RUB')
        avail = offers.get('availability', '')
        if 'InStock' in avail:
            pd.stock_status = 'in_stock'
        elif 'OutOfStock' in avail:
            pd.stock_status = 'out_of_stock'

    # Rating
    agg = product.get('aggregateRating', {})
    if isinstance(agg, dict):
        pd.rating        = pd.rating        or str(agg.get('ratingValue', ''))
        pd.reviews_count = pd.reviews_count or str(agg.get('reviewCount', '') or agg.get('ratingCount', ''))

    pd.parse_method = pd.parse_method or 'json_ld'


# ─────────────────────────────────────────────────────────────────────────────
#  PER-DOMAIN PARSERLAR
# ─────────────────────────────────────────────────────────────────────────────

class BaseParser:
    """Barcha parserlar shu klassdan meros oladi."""
    domains: list = []

    def parse(self, soup: BeautifulSoup, url: str, resp: requests.Response) -> ProductData:
        pd = ProductData()
        pd.source_url = url
        pd.domain = re.sub(r'^www\.', '', urlparse(url).netloc.lower())

        # Avval JSON-LD va OG dan olishga harakat
        parse_json_ld(soup, pd, url)
        parse_open_graph(soup, pd)

        # Keyin domen-specific
        self._parse_specific(soup, url, resp, pd)

        # title uchun fallback
        if not pd.title:
            h1 = soup.find('h1')
            if h1:
                pd.title = clean_text(h1.get_text())
        if not pd.title:
            pd.title = get_meta(soup, name='title') or clean_text(
                soup.title.get_text() if soup.title else '')

        pd.parse_method = pd.parse_method or 'generic'
        return pd

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        """Har bir domen o'z methodini override qiladi."""
        pass

    @staticmethod
    def find_images_generic(soup: BeautifulSoup, url: str, max_count: int = MAX_IMAGES) -> list:
        """Sahifadan rasm URL larini umumiy usulda topadi."""
        images = []
        seen = set()

        def add(img_url):
            u = abs_url(img_url, url)
            if not u or u in seen:
                return
            # Avatar, logo, icon larni o'tkazib yuborish
            low = u.lower()
            if any(x in low for x in ['logo', 'icon', 'avatar', 'banner', 'sprite', '1x1', 'pixel']):
                return
            # Juda kichik rasm URL parametri
            if re.search(r'[wh](?:idth)?[=_](\d+)', low):
                m = re.search(r'[wh](?:idth)?[=_](\d+)', low)
                if m and int(m.group(1)) < 50:
                    return
            seen.add(u)
            images.append(u)

        # product-image, gallery, slider larni qidirish
        for sel in [
            '[class*="product-image"]', '[class*="gallery"]',
            '[class*="slider"]', '[class*="carousel"]',
            '[class*="photo"]', '[class*="preview"]',
            '[data-zoom-image]', '[data-src]',
        ]:
            for tag in soup.select(sel)[:max_count * 2]:
                for attr in ['data-zoom-image', 'data-src', 'data-lazy', 'src', 'href']:
                    v = tag.get(attr, '')
                    if v and (v.startswith('http') or v.startswith('//')):
                        add(v)

        # Oddiy img teglari — content area ichida
        main = soup.select_one('main, [class*="content"], [class*="product"], article')
        scope = main if main else soup
        for img in scope.find_all('img', limit=50):
            src = img.get('src', '') or img.get('data-src', '') or img.get('data-lazy', '')
            if not src:
                continue
            ext_match = re.search(r'\.(jpg|jpeg|png|webp)', src, re.IGNORECASE)
            if ext_match:
                add(src)

        return images[:max_count]


# ── Wildberries ───────────────────────────────────────────────────────────────
class WildberriesParser(BaseParser):
    domains = ['wildberries.ru', 'global.wildberries.ru']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'wildberries'

        # WB API orqali (article ID dan)
        article = self._extract_article(url)
        if article:
            pd.extra['wb_article'] = article
            api_data = self._fetch_wb_api(article)
            if api_data:
                pd.title        = api_data.get('name', pd.title)
                pd.brand        = api_data.get('brand', pd.brand)
                pd.price        = api_data.get('salePriceU', pd.price)
                pd.old_price    = api_data.get('priceU', pd.old_price)
                pd.rating       = str(api_data.get('reviewRating', pd.rating))
                pd.reviews_count = str(api_data.get('feedbacks', pd.reviews_count))
                # Basket'dan rasmlar
                imgs = self._get_wb_images(article)
                pd.images = imgs or pd.images
                pd.parse_method = 'wildberries_api'
                return

        # HTML fallback
        name = soup.select_one('.product-page__header h1')
        if name:
            pd.title = clean_text(name.get_text())
        price = soup.select_one('.price-block__final-price')
        if price:
            pd.price = extract_price(price.get_text())
        brand = soup.select_one('.product-page__brand-name, [class*="brand"]')
        if brand:
            pd.brand = clean_text(brand.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)

    @staticmethod
    def _extract_article(url: str) -> str | None:
        """WB URL dan article ID chiqaradi."""
        # /catalog/148302947/detail.aspx yoki /product/...148302947
        m = re.search(r'/(?:catalog|product)/(\d{6,})', url)
        if m:
            return m.group(1)
        # ID son oxirida
        m = re.search(r'(\d{8,})', url)
        return m.group(1) if m else None

    @staticmethod
    def _fetch_wb_api(article: str) -> dict:
        """WB mahsulot API so'rovi."""
        try:
            url = f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&nm={article}"
            resp = fetch_url(url)
            if resp and resp.status_code == 200:
                data = resp.json()
                products = data.get('data', {}).get('products', [])
                if products:
                    return products[0]
        except Exception:
            pass
        return {}

    @staticmethod
    def _get_wb_images(article: str) -> list:
        """WB CDN dan rasm URLlarini hisoblaydi."""
        try:
            art_int = int(article)
            vol = art_int // 100000
            part = art_int // 1000
            # WB CDN basket nomerlari 1-20
            images = []
            for basket_n in range(1, 21):
                basket = str(basket_n).zfill(2)
                for idx in range(1, MAX_IMAGES + 1):
                    img_url = (f"https://basket-{basket}.wb.ru/vol{vol}/part{part}/"
                               f"{article}/images/big/{idx}.webp")
                    images.append(img_url)
            # Aslida tekshirish kerak, lekin 1-4 rasm odatda bor
            return [f"https://basket-01.wb.ru/vol{vol}/part{part}/{article}/images/big/{i}.webp"
                    for i in range(1, 5)]
        except Exception:
            return []


# ── Ozon ──────────────────────────────────────────────────────────────────────
class OzonParser(BaseParser):
    domains = ['ozon.ru', 'ozon.by', 'ozon.kz', 'am.ozon.com', 'uz.ozon.com']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'ozon'

        # Ozon JS da ma'lumot saqlaydi — state dan olish
        state_data = self._extract_state(resp.text if resp else '')
        if state_data:
            pd.title        = state_data.get('name', pd.title)
            pd.brand        = state_data.get('brand', pd.brand)
            pd.price        = state_data.get('price', pd.price)
            pd.rating       = state_data.get('rating', pd.rating)
            pd.reviews_count = state_data.get('reviews', pd.reviews_count)
            if state_data.get('images'):
                pd.images = state_data['images']
            pd.parse_method = 'ozon_state'
        else:
            # HTML fallback
            title = soup.select_one('h1[class*="ql-"]') or soup.find('h1')
            if title:
                pd.title = clean_text(title.get_text())
            # Ozon-specific selectorlar
            for sel in [
                '[class*="price"]',
                'span[class*="tsHeadline"]',
            ]:
                el = soup.select_one(sel)
                if el:
                    price_text = clean_text(el.get_text())
                    if re.search(r'\d', price_text):
                        pd.price = extract_price(price_text)
                        break

            if not pd.images:
                pd.images = self.find_images_generic(soup, url)

    @staticmethod
    def _extract_state(html: str) -> dict:
        """Ozon sahifasi JS state dan ma'lumot chiqaradi."""
        result = {}
        try:
            # __NEXT_DATA__ yoki similar
            m = re.search(r'window\.__NEXT_DATA__\s*=\s*(\{.+?\})(?=;\s*</script>)',
                          html, re.DOTALL)
            if not m:
                m = re.search(r'window\.__OZON_STATE__\s*=\s*(\{.+?\})\s*;',
                              html, re.DOTALL)
            if m:
                data = json.loads(m.group(1))
                # Chuqur qidirish
                def find_product(obj, depth=0):
                    if depth > 8 or not isinstance(obj, (dict, list)):
                        return None
                    if isinstance(obj, dict):
                        if obj.get('@type', '') == 'Product' or 'itemId' in obj:
                            return obj
                        for v in obj.values():
                            r = find_product(v, depth + 1)
                            if r:
                                return r
                    elif isinstance(obj, list):
                        for item in obj[:5]:
                            r = find_product(item, depth + 1)
                            if r:
                                return r
                    return None

                product = find_product(data)
                if product:
                    result['name']  = product.get('name', '')
                    result['price'] = extract_price(str(product.get('price', '')))
        except Exception:
            pass
        return result


# ── Yandex Market ─────────────────────────────────────────────────────────────
class YandexMarketParser(BaseParser):
    domains = ['market.yandex.ru', 'market.yandex.uz']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'yandex_market'

        # Yandex skidki + main narx
        price_el = (
            soup.select_one('[data-auto="snippet-price-current"]') or
            soup.select_one('[class*="priceValue"]') or
            soup.select_one('.price')
        )
        if price_el:
            pd.price = extract_price(price_el.get_text())

        brand_el = (
            soup.select_one('[data-auto="brand-name"]') or
            soup.select_one('[itemprop="brand"]')
        )
        if brand_el:
            pd.brand = clean_text(brand_el.get_text())

        # Xususiyatlar jadvali
        specs = {}
        for row in soup.select('[class*="specTable"] tr, [data-auto="spec"] li'):
            cells = row.find_all(['td', 'dd', 'span'])
            if len(cells) >= 2:
                k = clean_text(cells[0].get_text())
                v = clean_text(cells[1].get_text())
                if k and v:
                    specs[k] = v
        if specs:
            pd.specs.update(specs)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Avito ─────────────────────────────────────────────────────────────────────
class AvitoParser(BaseParser):
    domains = ['avito.ru', 'm.avito.ru']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'avito'
        title = soup.select_one('[class*="title-info-title"], h1[itemprop="name"]')
        if title:
            pd.title = clean_text(title.get_text())
        price = soup.select_one('[class*="price-value"], [itemprop="price"]')
        if price:
            pd.price = extract_price(price.get('content', '') or price.get_text())
        desc = soup.select_one('[class*="description-title"], [itemprop="description"]')
        if desc:
            pd.description = clean_text(desc.get_text())[:1000]
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Megamarket (SberMegamarket) ───────────────────────────────────────────────
class MegamarketParser(BaseParser):
    domains = ['megamarket.ru', 'sbermegamarket.ru']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'megamarket'
        title = soup.select_one('.pdp-header__title, h1')
        if title:
            pd.title = clean_text(title.get_text())
        price = soup.select_one('[class*="pdp-sales-price"], [class*="price"]')
        if price:
            pd.price = extract_price(price.get_text())
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Amazon ────────────────────────────────────────────────────────────────────
class AmazonParser(BaseParser):
    domains = ['amazon.com', 'amazon.de', 'amazon.co.uk', 'amazon.fr']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'amazon'
        title = soup.select_one('#productTitle, [data-feature-name="title"] h1')
        if title:
            pd.title = clean_text(title.get_text())
        price = (
            soup.select_one('.a-price .a-offscreen') or
            soup.select_one('#price_inside_buybox') or
            soup.select_one('[class*="apexPriceToPay"]')
        )
        if price:
            pd.price = extract_price(price.get_text())
        brand = soup.select_one('#bylineInfo, [class*="byline"]')
        if brand:
            pd.brand = clean_text(brand.get_text()).replace('Brand:', '').strip()
        asin = re.search(r'/dp/([A-Z0-9]{10})', url)
        if asin:
            pd.sku = pd.sku or asin.group(1)
            pd.extra['asin'] = asin.group(1)
        # Rasmlar
        img_data = re.search(r'"colorImages":\s*\{"initial":\s*(\[.+?\])\}', resp.text or '')
        if img_data:
            try:
                imgs = json.loads(img_data.group(1))
                for img in imgs[:MAX_IMAGES]:
                    hi = img.get('hiRes') or img.get('large') or img.get('main', '')
                    if hi and hi not in pd.images:
                        pd.images.append(hi)
            except Exception:
                pass
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── AliExpress ────────────────────────────────────────────────────────────────
class AliexpressParser(BaseParser):
    domains = ['aliexpress.com', 'aliexpress.ru', 'aliexpress.us']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'aliexpress'
        # AE JS state
        m = re.search(r'window\.runParams\s*=\s*(\{.+?\});\s*</script>', resp.text or '', re.DOTALL)
        if not m:
            m = re.search(r'"productInfoComponent":\s*(\{[^}]+\})', resp.text or '')
        if m:
            try:
                data = json.loads(m.group(1))
                comp = data.get('productInfoComponent', data)
                pd.title = comp.get('subject', pd.title)
                price_comp = data.get('priceComponent', {})
                pd.price = extract_price(str(price_comp.get('discountPrice', {}).get('value', '')))
            except Exception:
                pass
        if not pd.title:
            h1 = soup.find('h1')
            if h1:
                pd.title = clean_text(h1.get_text())
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Rozetka ───────────────────────────────────────────────────────────────────
class RozetkaParser(BaseParser):
    domains = ['rozetka.com.ua']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'rozetka'
        title = soup.select_one('h1.product__title, h1[class*="title"]')
        if title:
            pd.title = clean_text(title.get_text())
        price = soup.select_one('[class*="product-price__big"], p.price__big')
        if price:
            pd.price = extract_price(price.get_text())
        # Xususiyatlar
        for row in soup.select('.characteristics-full__item'):
            label = row.select_one('.characteristics-full__label')
            value = row.select_one('.characteristics-full__value')
            if label and value:
                pd.specs[clean_text(label.get_text())] = clean_text(value.get_text())
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Prom.ua ───────────────────────────────────────────────────────────────────
class PromUaParser(BaseParser):
    domains = ['prom.ua']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'prom_ua'
        title = soup.select_one('[class*="product-name"], h1[data-qaid="product_name"]')
        if title:
            pd.title = clean_text(title.get_text())
        price = soup.select_one('[data-qaid="product_price"], [class*="price"]')
        if price:
            pd.price = extract_price(price.get_text())
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Kosadakashop.ru (brend o'z saytlari) ─────────────────────────────────────
class KosadakaShopParser(BaseParser):
    domains = ['kosadakashop.ru']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'kosadakashop'
        pd.brand = 'KOSADAKA'
        title = soup.select_one('h1.product_title, h1[class*="title"]')
        if title:
            pd.title = clean_text(title.get_text())
        price = soup.select_one('.price, .woocommerce-Price-amount')
        if price:
            pd.price = extract_price(price.get_text())
        # WooCommerce gallery
        gallery = soup.select('.woocommerce-product-gallery__image a')
        for a in gallery[:MAX_IMAGES]:
            href = a.get('href', '')
            if href:
                pd.images.append(abs_url(href, url))
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Rybalkashop, fishing stores uchun umumiy ─────────────────────────────────
class FishingStoreParser(BaseParser):
    """Ko'pchilik mayda baliqchilik do'konlari uchun umumiy parser."""
    domains = []  # Dinamik qo'shiladi

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'fishing_store_generic'

        # Title: JSON-LD dan olingan bo'lishi kerak, aks holda h1
        if not pd.title:
            h1 = soup.find('h1')
            if h1:
                pd.title = clean_text(h1.get_text())

        # Narx — turli selectorlar
        for price_sel in [
            '.product-price', '.price', '[class*="price__value"]',
            '[class*="product__price"]', '[itemprop="price"]',
            '.cost', '.sticker', 'span.amount',
        ]:
            el = soup.select_one(price_sel)
            if el:
                price_text = clean_text(el.get('content', '') or el.get_text())
                if re.search(r'\d{2,}', price_text):
                    pd.price = extract_price(price_text)
                    break

        # SKU / artikul
        for sku_sel in [
            '[itemprop="sku"]', '[class*="sku"]', '[class*="article"]',
            '[class*="artikul"]', '[class*="vendor"]',
        ]:
            el = soup.select_one(sku_sel)
            if el:
                txt = clean_text(el.get('content', '') or el.get_text())
                if txt and len(txt) < 50:
                    pd.sku = pd.sku or txt
                    break

        # Tavsif
        for desc_sel in [
            '[itemprop="description"]', '.product-description',
            '[class*="description"]', '.tab-content',
        ]:
            el = soup.select_one(desc_sel)
            if el:
                pd.description = pd.description or clean_text(el.get_text())[:2000]
                break

        # Xususiyatlar jadvali
        for table_sel in [
            '.characteristics', 'table.product-attrs',
            '[class*="spec"]', '.features',
        ]:
            for row in soup.select(f'{table_sel} tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    k = clean_text(cells[0].get_text())
                    v = clean_text(cells[1].get_text())
                    if k and v and k != v:
                        pd.specs[k] = v

        # Breadcrumbs
        bc = soup.select_one('[class*="breadcrumb"], nav[aria-label*="bread"]')
        if bc:
            pd.breadcrumbs = ' > '.join(
                clean_text(a.get_text()) for a in bc.find_all('a')
            )

        # Rasmlar
        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── Universal fallback ────────────────────────────────────────────────────────
class UniversalParser(BaseParser):
    """Barcha noma'lum domenlar uchun."""
    domains = ['*']

    def _parse_specific(self, soup, url, resp, pd: ProductData):
        pd.parse_method = 'universal'
        fallback = FishingStoreParser()
        fallback._parse_specific(soup, url, resp, pd)


# ─────────────────────────────────────────────────────────────────────────────
#  PARSER REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

_PARSERS: list[BaseParser] = [
    WildberriesParser(),
    OzonParser(),
    YandexMarketParser(),
    AvitoParser(),
    MegamarketParser(),
    AmazonParser(),
    AliexpressParser(),
    RozetkaParser(),
    PromUaParser(),
    KosadakaShopParser(),
    FishingStoreParser(),
    UniversalParser(),
]

# Domen → parser mapping (tez qidirish uchun)
_DOMAIN_MAP: dict[str, BaseParser] = {}
for _p in _PARSERS:
    for _d in _p.domains:
        if _d != '*':
            _DOMAIN_MAP[_d] = _p

_FISHING_KEYWORDS = re.compile(
    r'(fish|rybalk|rybolov|snast|priman|spinning|vobler|blesn|shnur)',
    re.IGNORECASE
)


def get_parser(domain: str) -> BaseParser:
    """Domenge mos parser qaytaradi."""
    # To'liq moslik
    if domain in _DOMAIN_MAP:
        return _DOMAIN_MAP[domain]
    # Subdomen tekshirish (dobryanka.59rost.ru → 59rost.ru)
    parts = domain.split('.')
    for i in range(1, len(parts)):
        parent = '.'.join(parts[i:])
        if parent in _DOMAIN_MAP:
            return _DOMAIN_MAP[parent]
    # Baliqchilik saytlari uchun FishingStoreParser
    if _FISHING_KEYWORDS.search(domain):
        return _PARSERS[-2]  # FishingStoreParser
    return _PARSERS[-1]  # UniversalParser


# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE (SCRAPED.DB)
# ─────────────────────────────────────────────────────────────────────────────

def init_scraped_db(db_path: str = DST_DB):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS scraped_products (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,

        -- Manba ma'lumot (clean_products dan)
        src_brand       TEXT,
        src_barcode     TEXT,
        src_sku         TEXT,
        src_product_id  TEXT,
        src_product_name TEXT,
        source_url      TEXT,
        domain          TEXT,

        -- Saytdan yig'ilgan
        title           TEXT,
        brand           TEXT,
        sku             TEXT,
        barcode         TEXT,
        price           TEXT,
        old_price       TEXT,
        currency        TEXT,
        description     TEXT,
        rating          TEXT,
        reviews_count   TEXT,
        stock_status    TEXT,
        category        TEXT,
        breadcrumbs     TEXT,
        images_json     TEXT,   -- JSON array
        specs_json      TEXT,   -- JSON object
        extra_json      TEXT,   -- qo'shimcha

        parse_method    TEXT,
        http_status     INTEGER,
        scraped_at      TEXT DEFAULT (datetime('now')),

        UNIQUE(source_url)
    );

    CREATE TABLE IF NOT EXISTS scrape_queue (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        src_brand       TEXT,
        src_barcode     TEXT,
        src_sku         TEXT,
        src_product_id  TEXT,
        src_product_name TEXT,
        url             TEXT NOT NULL UNIQUE,
        domain          TEXT,
        status          TEXT DEFAULT 'pending',
                        -- pending | done | error | skip
        attempt         INTEGER DEFAULT 0,
        error_msg       TEXT,
        added_at        TEXT DEFAULT (datetime('now')),
        updated_at      TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS downloaded_images (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id  INTEGER REFERENCES scraped_products(id),
        src_brand   TEXT,
        src_barcode TEXT,
        src_sku     TEXT,
        image_url   TEXT,
        local_path  TEXT,
        file_size   INTEGER,
        downloaded_at TEXT DEFAULT (datetime('now')),
        UNIQUE(image_url)
    );

    CREATE INDEX IF NOT EXISTS idx_sq_status  ON scrape_queue(status);
    CREATE INDEX IF NOT EXISTS idx_sq_domain  ON scrape_queue(domain);
    CREATE INDEX IF NOT EXISTS idx_sp_brand   ON scraped_products(src_brand);
    CREATE INDEX IF NOT EXISTS idx_sp_barcode ON scraped_products(src_barcode);
    CREATE INDEX IF NOT EXISTS idx_sp_url     ON scraped_products(source_url);
    CREATE INDEX IF NOT EXISTS idx_di_product ON downloaded_images(product_id);
    """)
    conn.commit()
    conn.close()
    log.info(f"[DB] scraped.db tayyor: {db_path}")


def load_queue_from_pipeline(src_db: str = SRC_DB, dst_db: str = DST_DB,
                              domain_filter: str = None):
    """clean_products dan scrape_queue ga URL lar yuklaydi."""
    if not os.path.exists(src_db):
        log.error(f"pipeline.db topilmadi: {src_db}")
        return 0

    src_conn = sqlite3.connect(src_db)
    src_conn.row_factory = sqlite3.Row
    dst_conn = sqlite3.connect(dst_db, timeout=30)
    dst_conn.execute("PRAGMA journal_mode=WAL")

    query = """
        SELECT brand, barcode, sku, product_id, product_name,
               url, url_domain
        FROM clean_products
        WHERE url IS NOT NULL AND url != ''
    """
    params = []
    if domain_filter:
        query += " AND url_domain LIKE ?"
        params.append(f'%{domain_filter}%')

    rows = src_conn.execute(query, params).fetchall()
    added = skipped = 0

    for row in rows:
        try:
            cur = dst_conn.execute("""
                INSERT OR IGNORE INTO scrape_queue
                    (src_brand, src_barcode, src_sku, src_product_id,
                     src_product_name, url, domain)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row['brand'], row['barcode'], row['sku'], row['product_id'],
                  row['product_name'], row['url'], row['url_domain']))
            if cur.rowcount > 0:
                added += 1
            else:
                skipped += 1
        except Exception as e:
            log.warning(f"Queue insert xato: {e}")

    dst_conn.commit()
    src_conn.close()
    dst_conn.close()
    log.info(f"[Queue] {added} yangi URL qo'shildi, {skipped} allaqachon bor")
    return added


def get_pending_urls(db_path: str, limit: int = 50) -> list:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM scrape_queue
        WHERE status IN ('pending', 'error')
          AND attempt < ?
        ORDER BY id ASC
        LIMIT ?
    """, (MAX_RETRIES, limit)).fetchall()
    conn.close()
    return rows


def mark_queue(db_path: str, queue_id: int, status: str,
               error_msg: str = None, attempt: int = None):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    params = [status, datetime.now(UTC).isoformat()]
    sql = "UPDATE scrape_queue SET status=?, updated_at=?"
    if error_msg:
        sql += ", error_msg=?"
        params.append(error_msg[:500])
    if attempt is not None:
        sql += ", attempt=?"
        params.append(attempt)
    sql += " WHERE id=?"
    params.append(queue_id)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def save_scraped_product(db_path: str, queue_row, pd_obj: ProductData,
                         http_status: int) -> int | None:
    """Scraped mahsulotni DB ga saqlaydi."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    d = pd_obj.to_dict()
    try:
        cur = conn.execute("""
            INSERT OR REPLACE INTO scraped_products
                (src_brand, src_barcode, src_sku, src_product_id, src_product_name,
                 source_url, domain,
                 title, brand, sku, barcode, price, old_price, currency,
                 description, rating, reviews_count, stock_status,
                 category, breadcrumbs,
                 images_json, specs_json, extra_json,
                 parse_method, http_status, scraped_at)
            VALUES (?,?,?,?,?, ?,?, ?,?,?,?,?,?,?, ?,?,?,?, ?,?, ?,?,?, ?,?,datetime('now'))
        """, (
            queue_row['src_brand'], queue_row['src_barcode'], queue_row['src_sku'],
            queue_row['src_product_id'], queue_row['src_product_name'],
            d['source_url'], d['domain'],
            d['title'], d['brand'], d['sku'], d['barcode'],
            d['price'], d['old_price'], d['currency'],
            d['description'], d['rating'], d['reviews_count'], d['stock_status'],
            d['category'], d['breadcrumbs'],
            d['images_json'], d['specs_json'], d['extra_json'],
            d['parse_method'], http_status
        ))
        conn.commit()
        product_db_id = cur.lastrowid
        conn.close()
        return product_db_id
    except Exception as e:
        log.error(f"save_scraped_product xato: {e}")
        conn.close()
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  RASM YUKLAB OLISH
# ─────────────────────────────────────────────────────────────────────────────

def safe_filename(text: str) -> str:
    """Fayl nomiga xavfsiz matn."""
    return re.sub(r'[^\w\-_]', '', text)[:50]


def get_img_dir(brand: str) -> Path:
    """Brand uchun rasm papkasini qaytaradi (mavjud bo'lmasa yaratadi)."""
    folder = Path(IMG_DIR) / safe_filename(brand.upper())
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def download_image(img_url: str, local_path: str) -> int:
    """Rasmni yuklab oladi. Fayl hajmini qaytaradi (0 = xato)."""
    try:
        session = get_session()
        resp = session.get(img_url, timeout=15, stream=True)
        if resp.status_code != 200:
            return 0
        content_type = resp.headers.get('Content-Type', '')
        if 'image' not in content_type and 'octet-stream' not in content_type:
            return 0
        total = 0
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
                total += len(chunk)
        if total < 1000:   # 1KB dan kichik — kontent emas
            os.remove(local_path)
            return 0
        return total
    except Exception as e:
        log.debug(f"  Rasm yuklab bo'lmadi: {img_url[:60]}: {e}")
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass
        return 0


def save_images(db_path: str, product_db_id: int, queue_row,
                images: list, brand: str) -> int:
    """Mahsulot rasmlarini yuklab saqlaydi."""
    if not images or not DOWNLOAD_IMAGES:
        return 0

    folder    = get_img_dir(brand)
    sku       = safe_filename(queue_row['src_sku'] or '')
    barcode   = safe_filename(queue_row['src_barcode'] or '')
    prefix    = f"{sku}-{barcode}" if sku or barcode else safe_filename(
        queue_row['src_product_name'] or 'product')

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # Mavjud rasm tartib raqamini aniqlash
    cur = conn.execute(
        "SELECT COUNT(*) FROM downloaded_images WHERE src_barcode=?",
        (queue_row['src_barcode'],)
    )
    start_idx = cur.fetchone()[0] + 1

    saved = 0
    for i, img_url in enumerate(images[:MAX_IMAGES], start=start_idx):
        if not img_url:
            continue
        # Ext aniqlash
        ext = 'jpg'
        url_path = urlparse(img_url).path.lower()
        for possible_ext in ['webp', 'png', 'jpeg', 'jpg', 'gif']:
            if url_path.endswith('.' + possible_ext):
                ext = possible_ext
                break

        filename  = f"{prefix}-{i}.{ext}"
        local_path = str(folder / filename)

        # Allaqachon yuklanganmi?
        exists = conn.execute(
            "SELECT id FROM downloaded_images WHERE image_url=?", (img_url,)
        ).fetchone()
        if exists:
            saved += 1
            continue

        size = download_image(img_url, local_path)
        if size > 0:
            conn.execute("""
                INSERT OR IGNORE INTO downloaded_images
                    (product_id, src_brand, src_barcode, src_sku,
                     image_url, local_path, file_size)
                VALUES (?,?,?,?, ?,?,?)
            """, (product_db_id, queue_row['src_brand'], queue_row['src_barcode'],
                  queue_row['src_sku'], img_url, local_path, size))
            conn.commit()
            saved += 1
            log.debug(f"    🖼 Rasm saqlandi: {filename} ({size//1024}KB)")
        else:
            log.debug(f"    ✗ Rasm yuklanmadi: {img_url[:60]}")

        time.sleep(0.3)  # rasm so'rovlari orasida kichik pauza

    conn.close()
    return saved


# ─────────────────────────────────────────────────────────────────────────────
#  ASOSIY SCRAPING FUNKSIYA (bitta URL)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_one(queue_row, db_path: str) -> dict:
    """Bitta URL ni parse qilib natijasini saqlaydi."""
    url       = queue_row['url']
    brand     = queue_row['src_brand'] or ''
    queue_id  = queue_row['id']
    attempt   = queue_row['attempt'] + 1

    result = {'url': url, 'status': 'error', 'title': '', 'images': 0}

    try:
        resp = fetch_url(url)

        if resp is None:
            mark_queue(db_path, queue_id, 'error',
                       error_msg='Connection failed', attempt=attempt)
            result['error'] = 'connection_failed'
            return result

        http_status = resp.status_code

        # 404, 410 — URL mavjud emas
        if http_status in (404, 410, 403):
            mark_queue(db_path, queue_id, 'skip',
                       error_msg=f'HTTP {http_status}', attempt=attempt)
            result['status'] = 'skip'
            result['error']  = f'http_{http_status}'
            return result

        # 429, 503 — rate limit
        if http_status in (429, 503, 520, 521, 522, 523, 524):
            wait = 60 + random.uniform(0, 30)
            log.warning(f"  ⚠ HTTP {http_status} rate limit, {wait:.0f}s kutmoqda...")
            time.sleep(wait)
            mark_queue(db_path, queue_id, 'error',
                       error_msg=f'Rate limit HTTP {http_status}', attempt=attempt)
            result['error'] = f'rate_limit_{http_status}'
            return result

        if http_status != 200:
            mark_queue(db_path, queue_id, 'error',
                       error_msg=f'HTTP {http_status}', attempt=attempt)
            result['error'] = f'http_{http_status}'
            return result

        # HTML parse
        content_type = resp.headers.get('Content-Type', '')
        if 'html' not in content_type and 'json' not in content_type:
            mark_queue(db_path, queue_id, 'skip',
                       error_msg=f'Not HTML: {content_type}', attempt=attempt)
            result['status'] = 'skip'
            return result

        soup = build_soup(resp)

        # Parser tanlash
        domain = re.sub(r'^www\.', '', urlparse(url).netloc.lower())
        parser = get_parser(domain)
        pd_obj = parser.parse(soup, url, resp)

        # Parser_v4 dagi kanoniklashtirish yondashuvi bo'yicha qo'shimcha kalitlar.
        pd_obj.extra.setdefault('src_sku_key', canonical_number_key(queue_row['src_sku']))
        pd_obj.extra.setdefault('src_barcode_key', canonical_number_key(queue_row['src_barcode']))
        pd_obj.extra.setdefault('parsed_sku_key', canonical_number_key(pd_obj.sku))
        pd_obj.extra.setdefault('parsed_barcode_key', canonical_number_key(pd_obj.barcode))

        # Saqlash
        product_db_id = save_scraped_product(db_path, queue_row, pd_obj, http_status)

        # Rasmlarni yuklab olish
        imgs_saved = 0
        if product_db_id and pd_obj.images:
            imgs_saved = save_images(db_path, product_db_id, queue_row,
                                     pd_obj.images, brand)

        mark_queue(db_path, queue_id, 'done', attempt=attempt)

        result.update({
            'status': 'done',
            'title':  pd_obj.title[:80] if pd_obj.title else '',
            'price':  pd_obj.price,
            'images': imgs_saved,
            'parser': pd_obj.parse_method,
        })
        log.info(
            f"  ✓ [{pd_obj.parse_method:<20}] "
            f"{result['title'][:50]:<50} | "
            f"narx:{pd_obj.price:<8} | "
            f"🖼{imgs_saved}"
        )
        return result

    except Exception as e:
        tb = traceback.format_exc()
        log.error(f"  ✗ Xato {url[:60]}: {e}\n{tb[:300]}")
        mark_queue(db_path, queue_id, 'error',
                   error_msg=str(e)[:400], attempt=attempt)
        result['error'] = str(e)[:100]
        return result


# ─────────────────────────────────────────────────────────────────────────────
#  PARALLEL PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(domain_filter: str = None, limit: int = None,
                workers: int = MAX_WORKERS, no_images: bool = False):
    global DOWNLOAD_IMAGES
    if no_images:
        DOWNLOAD_IMAGES = False

    # DB init
    init_scraped_db()
    load_queue_from_pipeline(domain_filter=domain_filter)

    total_done = total_error = total_skip = 0
    batch_n = 0

    log.info(f"\n{'='*60}")
    log.info(f"  SCRAPER BOSHLANDI | workers={workers} | images={DOWNLOAD_IMAGES}")
    log.info(f"{'='*60}\n")

    while True:
        pending = get_pending_urls(DST_DB, limit=workers * 5 if not limit else limit)
        if not pending:
            log.info("Barcha URL lar qayta ishlandi. Tugadi.")
            break

        # Parallel scraping
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for row in pending:
                # Thread ga yetkazishdan oldin kichik farqli delay
                time.sleep(random.uniform(0.5, 1.5))
                f = executor.submit(scrape_one, row, DST_DB)
                futures[f] = row

            for f in as_completed(futures):
                result = f.result()
                if result['status'] == 'done':
                    total_done += 1
                elif result['status'] == 'skip':
                    total_skip += 1
                else:
                    total_error += 1

        batch_n += 1

        # Batch holati
        log.info(
            f"\n{'─'*50}\n"
            f"  BATCH #{batch_n} | done:{total_done} | "
            f"error:{total_error} | skip:{total_skip}\n"
            f"{'─'*50}"
        )

        # Batch orasidagi pauza
        pause = random.uniform(DELAY_MIN * workers, DELAY_MAX * workers)
        log.info(f"  {pause:.0f}s tanaffus...")
        time.sleep(pause)

        if limit and (total_done + total_skip) >= limit:
            break

    # Yakuniy statistika
    log.info(
        f"\n{'='*60}\n"
        f"  SCRAPING YAKUNLANDI\n"
        f"  ✓ done  : {total_done}\n"
        f"  ✗ error : {total_error}\n"
        f"  – skip  : {total_skip}\n"
        f"{'='*60}\n"
    )
    show_stats()


# ─────────────────────────────────────────────────────────────────────────────
#  STATISTIKA
# ─────────────────────────────────────────────────────────────────────────────

def show_stats():
    if not os.path.exists(DST_DB):
        print("scraped.db topilmadi.")
        return

    conn = sqlite3.connect(DST_DB)
    print(f"\n{'─'*55}")
    print("  SCRAPING HOLATI")
    print(f"{'─'*55}")

    # Queue
    for status, cnt in conn.execute(
        "SELECT status, COUNT(*) FROM scrape_queue GROUP BY status"
    ).fetchall():
        print(f"  queue {status:<10}: {cnt:>6}")

    print(f"{'─'*55}")

    # Scraped products by brand
    for brand, cnt in conn.execute(
        "SELECT src_brand, COUNT(*) FROM scraped_products GROUP BY src_brand"
    ).fetchall():
        print(f"  scraped {brand:<8}: {cnt:>6} mahsulot")

    # Images
    img_count = conn.execute("SELECT COUNT(*) FROM downloaded_images").fetchone()[0]
    img_size = conn.execute(
        "SELECT SUM(file_size) FROM downloaded_images").fetchone()[0] or 0
    print(f"{'─'*55}")
    print(f"  Jami rasmlar  : {img_count:>6} ta ({img_size//1024//1024} MB)")
    print(f"{'─'*55}\n")
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]

    domain_filter = None
    limit_n       = None
    workers_n     = MAX_WORKERS
    no_images     = False

    if '--domain' in args:
        i = args.index('--domain')
        domain_filter = args[i + 1]
        print(f"Domen filtr: {domain_filter}")

    if '--limit' in args:
        i = args.index('--limit')
        limit_n = int(args[i + 1])
        print(f"Limit: {limit_n}")

    if '--workers' in args:
        i = args.index('--workers')
        workers_n = int(args[i + 1])

    if '--no-images' in args:
        no_images = True

    if '--stats' in args:
        show_stats()

    elif '--retry-errors' in args:
        # Error larni pending ga qaytarish
        conn = sqlite3.connect(DST_DB)
        conn.execute("UPDATE scrape_queue SET status='pending', attempt=0 WHERE status='error'")
        conn.commit()
        cnt = conn.execute("SELECT changes()").fetchone()[0]
        conn.close()
        print(f"{cnt} error → pending qilindi")
        run_scraper(domain_filter, limit_n, workers_n, no_images)

    elif '--init' in args:
        init_scraped_db()
        load_queue_from_pipeline(domain_filter=domain_filter)

    else:
        run_scraper(domain_filter, limit_n, workers_n, no_images)
