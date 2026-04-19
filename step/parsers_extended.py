"""
KENGAYTIRILGAN PARSERLAR — jsonlar.txt dagi BARCHA domenlar uchun
=================================================================
Har bir parser uchun quyidagi strategiya:
  1. JSON-LD (schema.org Product) — agar bor bo'lsa, eng ishonchli
  2. Open Graph meta teglari
  3. Domen-specific HTML selectorlar
  4. itemprop/microdata fallback
  5. Generic h1 + price + image fallback

Yangi parserlar:
  SimaLandParser, MeshokParser, HftParser, BiggameParser,
  TrophyFishingParser, OlympsportParser, SinaLandParser,
  WooCommerceParser (ko'p mayda saytlar uchun),
  OpenCartParser (ko'p RU baliqchilik saytlari uchun),
  BitrixParser (1C-Bitrix saytlari uchun),
  ReviewsYandexParser, RybalkashopParser, FishbonParser,
  RatterbaitsParser, va boshqalar.
"""

import re
import json
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

# ─── Agar step6_scraper.py bilan birga ishlatiladigan bo'lsa ────────────────
try:
    from step6_scraper import (
        BaseParser, ProductData, clean_text, extract_price,
        abs_url, get_meta, parse_json_ld, parse_open_graph,
        find_product_schema, extract_json_ld, fetch_url, MAX_IMAGES
    )
    STANDALONE = False
except ImportError:
    # Standalone test uchun minimal shim
    STANDALONE = True
    MAX_IMAGES = 10

    def clean_text(text):
        if not text:
            return ''
        return re.sub(r'\s+', ' ', text.strip())

    def extract_price(text):
        if not text:
            return ''
        digits = re.sub(r'[^\d.,]', '', text.replace(' ', '').replace('\xa0', ''))
        digits = digits.replace(',', '.')
        parts = digits.split('.')
        if len(parts) > 2:
            digits = ''.join(parts[:-1]) + '.' + parts[-1]
        return digits

    def abs_url(url, base_url):
        if not url:
            return ''
        if url.startswith('//'):
            return 'https:' + url
        if url.startswith('http'):
            return url
        return urljoin(base_url, url)

    def get_meta(soup, name=None, prop=None, itemprop=None):
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

    def extract_json_ld(soup):
        results = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.get_text(strip=True))
                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)
            except Exception:
                pass
        return results

    def find_product_schema(json_ld_list):
        for item in json_ld_list:
            t = item.get('@type', '')
            types = [x.lower() for x in t] if isinstance(t, list) else [t.lower()]
            if 'product' in types:
                return item
            if isinstance(item, dict) and '@graph' in item:
                for node in item['@graph']:
                    if 'product' in node.get('@type', '').lower():
                        return node
        return {}

    def parse_json_ld(soup, pd, base_url):
        product = find_product_schema(extract_json_ld(soup))
        if not product:
            return
        pd['title'] = pd.get('title') or clean_text(product.get('name', ''))
        brand = product.get('brand', {})
        pd['brand'] = pd.get('brand') or clean_text(
            brand.get('name', '') if isinstance(brand, dict) else str(brand))
        pd['sku'] = pd.get('sku') or clean_text(product.get('sku', ''))
        offers = product.get('offers', {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            pd['price'] = pd.get('price') or extract_price(str(offers.get('price', '')))
        img = product.get('image')
        if isinstance(img, str) and img:
            pd.setdefault('images', []).append(abs_url(img, base_url))
        elif isinstance(img, list):
            for i in img:
                u = abs_url(i if isinstance(i, str) else i.get('url', ''), base_url)
                if u:
                    pd.setdefault('images', []).append(u)

    def parse_open_graph(soup, pd):
        pd['title'] = pd.get('title') or get_meta(soup, prop='og:title')
        pd['description'] = pd.get('description') or get_meta(soup, prop='og:description')
        img = get_meta(soup, prop='og:image')
        if img:
            pd.setdefault('images', []).append(img)

    class ProductData:
        def __init__(self):
            self.title = ''
            self.brand = ''
            self.sku = ''
            self.barcode = ''
            self.price = ''
            self.old_price = ''
            self.currency = 'RUB'
            self.description = ''
            self.rating = ''
            self.reviews_count = ''
            self.stock_status = ''
            self.category = ''
            self.breadcrumbs = ''
            self.images = []
            self.specs = {}
            self.extra = {}
            self.source_url = ''
            self.domain = ''
            self.parse_method = ''

    class BaseParser:
        domains = []

        def parse(self, soup, url, resp):
            pd = ProductData()
            pd.source_url = url
            pd.domain = re.sub(r'^www\.', '', urlparse(url).netloc.lower())
            parse_json_ld(soup, pd, url)
            parse_open_graph(soup, pd)
            self._parse_specific(soup, url, resp, pd)
            if not pd.title:
                h1 = soup.find('h1')
                if h1:
                    pd.title = clean_text(h1.get_text())
            if not pd.title:
                pd.title = get_meta(soup, name='title') or clean_text(
                    soup.title.get_text() if soup.title else '')
            pd.parse_method = pd.parse_method or 'generic'
            return pd

        def _parse_specific(self, soup, url, resp, pd):
            pass

        @staticmethod
        def find_images_generic(soup, url, max_count=MAX_IMAGES):
            images = []
            seen = set()
            def add(img_url):
                u = abs_url(img_url, url)
                if not u or u in seen:
                    return
                low = u.lower()
                if any(x in low for x in ['logo','icon','avatar','banner','sprite','1x1','pixel']):
                    return
                seen.add(u)
                images.append(u)
            for sel in ['[class*="product-image"]','[class*="gallery"]','[class*="slider"]',
                        '[class*="carousel"]','[class*="photo"]','[class*="preview"]',
                        '[data-zoom-image]','[data-src]']:
                for tag in soup.select(sel)[:max_count * 2]:
                    for attr in ['data-zoom-image','data-src','data-lazy','src','href']:
                        v = tag.get(attr, '')
                        if v and (v.startswith('http') or v.startswith('//')):
                            add(v)
            main = soup.select_one('main, [class*="content"], [class*="product"], article')
            scope = main if main else soup
            for img in scope.find_all('img', limit=50):
                src = img.get('src','') or img.get('data-src','') or img.get('data-lazy','')
                if src and re.search(r'\.(jpg|jpeg|png|webp)', src, re.IGNORECASE):
                    add(src)
            return images[:max_count]

    def fetch_url(url, referer=None, retry=0):
        import requests
        try:
            s = requests.Session()
            s.headers['User-Agent'] = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                       'AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36')
            return s.get(url, timeout=20, allow_redirects=True)
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════════════════
#  YORDAMCHI FUNKSIYALAR (KENGAYTIRILGAN)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_itemprop_product(soup, pd, base_url):
    """Microdata itemprop="..." dan mahsulot ma'lumotlarini oladi."""
    # Narx
    price_el = soup.find(itemprop='price')
    if price_el:
        val = price_el.get('content', '') or price_el.get_text()
        pd.price = pd.price or extract_price(val)

    # Nom
    name_el = soup.find(itemprop='name')
    if name_el:
        pd.title = pd.title or clean_text(name_el.get('content','') or name_el.get_text())

    # Brand
    brand_el = soup.find(itemprop='brand')
    if brand_el:
        inner = brand_el.find(itemprop='name')
        pd.brand = pd.brand or clean_text(
            (inner.get_text() if inner else '') or brand_el.get_text())

    # SKU
    sku_el = soup.find(itemprop='sku')
    if sku_el:
        pd.sku = pd.sku or clean_text(sku_el.get('content','') or sku_el.get_text())

    # Description
    desc_el = soup.find(itemprop='description')
    if desc_el:
        pd.description = pd.description or clean_text(desc_el.get_text())[:2000]

    # Images
    for img_el in soup.find_all(itemprop='image')[:MAX_IMAGES]:
        u = abs_url(img_el.get('src','') or img_el.get('content','') or
                    img_el.get('href',''), base_url)
        if u and u not in pd.images:
            pd.images.append(u)


def extract_specs_table(soup):
    """Xususiyatlar jadvalini oladi (umumiy usul)."""
    specs = {}
    for sel in [
        'table.product-attrs', 'table.specs', '.characteristics',
        '[class*="spec"]', '[class*="param"]', '[class*="feature"]',
        'dl', '.properties', '.tech-specs',
    ]:
        for row in soup.select(f'{sel} tr, {sel} dt, {sel} li'):
            cells = row.find_all(['th', 'td', 'dt', 'dd'])
            if len(cells) >= 2:
                k = clean_text(cells[0].get_text())
                v = clean_text(cells[1].get_text())
                if k and v and k != v and len(k) < 100:
                    specs[k] = v
        # dl/dt/dd
        dts = soup.select(f'{sel} dt')
        dds = soup.select(f'{sel} dd')
        for dt, dd in zip(dts, dds):
            k = clean_text(dt.get_text())
            v = clean_text(dd.get_text())
            if k and v:
                specs[k] = v
    return specs


def extract_breadcrumbs(soup):
    """Breadcrumbs navini oladi."""
    for sel in [
        '[itemtype*="BreadcrumbList"]', '[class*="breadcrumb"]',
        'nav[aria-label*="bread"]', '.crumbs', '.nav-path',
    ]:
        bc = soup.select_one(sel)
        if bc:
            items = bc.find_all(['a', 'span', 'li'])
            parts = [clean_text(i.get_text()) for i in items if clean_text(i.get_text())]
            if parts:
                return ' > '.join(parts)
    return ''


def detect_woocommerce(soup):
    """Sahifa WooCommerce ekanligini aniqlaydi."""
    return bool(
        soup.find(class_=re.compile(r'woocommerce')) or
        soup.find('body', class_=re.compile(r'woocommerce')) or
        soup.find(class_='woocommerce-product-gallery') or
        soup.find('form', class_='cart')
    )


def detect_opencart(soup):
    """Sahifa OpenCart ekanligini aniqlaydi."""
    return bool(
        soup.find(id='product') or
        soup.find(class_=re.compile(r'product-info')) or
        soup.select_one('#content .product-info, .tab-content #tab-description')
    )


def detect_bitrix(soup):
    """Sahifa 1C-Bitrix ekanligini aniqlaydi."""
    return bool(
        soup.find(class_=re.compile(r'bx-')) or
        soup.find(id=re.compile(r'^bx_')) or
        soup.find('div', id='bx_left_nav')
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  KENGAYTIRILGAN PARSERLAR
# ═══════════════════════════════════════════════════════════════════════════════

# ── 1. SimaLand ───────────────────────────────────────────────────────────────
class SimaLandParser(BaseParser):
    """
    sima-land.ru — katta ulgurji do'kon.
    JSON-LD bor, lekin JS bilan ham yuklanadi.
    HTML da: h1.goods-name, .goods-price__main, .goods-description
    """
    domains = ['sima-land.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'simaland'

        title = soup.select_one('h1[class*="goods-name"], h1[class*="product-name"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for price_sel in [
            '[class*="goods-price__main"]', '[class*="product-price"]',
            '[class*="price__value"]', '[itemprop="price"]',
        ]:
            el = soup.select_one(price_sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        # SKU / artikul
        for sku_sel in ['[class*="article"]', '[class*="sku"]', '[itemprop="sku"]']:
            el = soup.select_one(sku_sel)
            if el:
                t = clean_text(el.get_text())
                if re.search(r'\d', t):
                    pd.sku = pd.sku or re.sub(r'[^\w\-]', '', t)[:30]
                    break

        # Description
        for desc_sel in ['[class*="goods-description"]', '[itemprop="description"]',
                         '.product-description', '[class*="description"]']:
            el = soup.select_one(desc_sel)
            if el:
                pd.description = pd.description or clean_text(el.get_text())[:2000]
                break

        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 2. Meshok.net ─────────────────────────────────────────────────────────────
class MeshokParser(BaseParser):
    """
    meshok.net — auksion/bahor sayt (Avito o'xshash).
    Sahifa: .lot-title, .price-number yoki .js-price, .lot-description
    """
    domains = ['meshok.net']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'meshok'

        title = (
            soup.select_one('.lot-title') or
            soup.select_one('[class*="lot-title"]') or
            soup.select_one('h1[itemprop="name"]') or
            soup.find('h1')
        )
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = (
            soup.select_one('.price-number') or
            soup.select_one('.js-price') or
            soup.select_one('[class*="price"]') or
            soup.find(itemprop='price')
        )
        if price:
            t = price.get('content','') or price.get_text()
            pd.price = pd.price or extract_price(t)

        desc = soup.select_one('.lot-description, [itemprop="description"], [class*="description"]')
        if desc:
            pd.description = pd.description or clean_text(desc.get_text())[:2000]

        # Lot raqami (SKU o'rniga)
        lot_m = re.search(r'/item/(\d+)', url)
        if lot_m:
            pd.sku = pd.sku or lot_m.group(1)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 3. HFT.ru ─────────────────────────────────────────────────────────────────
class HftParser(BaseParser):
    """
    hft.ru — baliqchilik/turizm do'koni. 1C-Bitrix asosida.
    HTML: h1.title, .price, .product-description, .product-props
    """
    domains = ['hft.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'hft'

        title = soup.select_one('h1.title, h1[class*="product"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for price_sel in [
            '.price__value', '.price', '[class*="price__current"]',
            '[itemprop="price"]',
        ]:
            el = soup.select_one(price_sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        # Bitrix product ID
        id_m = re.search(r'/(\d{5,})(?:/|$|\?)', url)
        if id_m:
            pd.extra['bitrix_id'] = id_m.group(1)

        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 4. Biggame.ru ──────────────────────────────────────────────────────────────
class BiggameParser(BaseParser):
    """
    biggame.ru — turizm/baliqchilik do'koni.
    1C-Bitrix asosida. JSON-LD bor.
    """
    domains = ['biggame.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'biggame'

        title = soup.select_one('h1.product-card-title, h1[class*="title"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for sel in [
            '.product-card__price-current', '.price-item', '[class*="price_value"]',
            '[itemprop="price"]',
        ]:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        parse_itemprop_product(soup, pd, url)
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 5. TrophyFishing (ko'p shaharlarda subdomains) ────────────────────────────
class TrophyFishingParser(BaseParser):
    """
    trophyfishing.ru va shaharlardagi subdomenlar:
    krasnoyarsk.trophyfishing.ru, ufa.trophyfishing.ru, volgograd.trophyfishing.ru
    1C-Bitrix asosida.
    """
    domains = ['trophyfishing.ru', 'krasnoyarsk.trophyfishing.ru',
               'ufa.trophyfishing.ru', 'volgograd.trophyfishing.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'trophyfishing'

        title = soup.select_one('h1.element-name, h1[class*="product"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for sel in ['.price', '.element-price', '[class*="price_value"]',
                    '[itemprop="price"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        parse_itemprop_product(soup, pd, url)
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 6. Olympsport.spb.ru ──────────────────────────────────────────────────────
class OlympsportParser(BaseParser):
    """
    olympsport.spb.ru — sport/baliqchilik. Tilda/custom asosida.
    """
    domains = ['olympsport.spb.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'olympsport'

        title = soup.select_one('.t-product__title, h1, [class*="product-title"]')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('.t-product__price, [class*="price"]')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 7. Reviews.yandex.ru ──────────────────────────────────────────────────────
class ReviewsYandexParser(BaseParser):
    """
    reviews.yandex.ru — bu mahsulot sahifasi emas, sharhlar sahifasi.
    Mahsulot nomini title dan, rasmni OG dan olamiz.
    """
    domains = ['reviews.yandex.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'reviews_yandex'
        pd.extra['note'] = 'reviews_page_not_product'

        # Mahsulot nomi title/h1 dan
        title = soup.select_one('h1, [class*="product-name"]')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        # Rating
        rating_el = soup.select_one('[class*="rating__value"], [class*="stars__value"]')
        if rating_el:
            pd.rating = clean_text(rating_el.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 8. WooCommerce Universal ──────────────────────────────────────────────────
class WooCommerceParser(BaseParser):
    """
    WooCommerce saytlari uchun universal parser.
    Ishlatiladi: kosadakashop.ru, fish-rod.com.ua, spinning.by,
    pikemaster.com.ua, rybalkashop.by, megafish.by, papafish.by,
    fishingstock.ua, va boshqalar.
    """
    domains = [
        'fish-rod.com.ua', 'spinning.by', 'pikemaster.com.ua',
        'rybalkashop.by', 'megafish.by', 'papafish.by', 'fishingstock.ua',
        'wobblershop.ru', 'spinningline.ru', 'ibis-gear.com', 'ibis.net.ua',
        'caiman-fishing.ru', 'caimanfishing.ru', 'vobler.by', 'kuldkalake.eu',
        'triturus-fishing.com', 'synthetic.ua', 'fishbon.eu',
        'rybalka-opt.com.ua', 'profish.ua', 'rybolov-expert.com.ua',
        'shimano.kiev.ua', 'xn----ctbibbzhmpe6am.com',
    ]

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'woocommerce'

        # Title
        title = soup.select_one(
            'h1.product_title, h1.entry-title, '
            '.product_title, [itemprop="name"] h1, h1'
        )
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        # Narx
        price = (
            soup.select_one('.woocommerce-Price-amount bdi') or
            soup.select_one('.woocommerce-Price-amount') or
            soup.select_one('p.price ins .amount') or
            soup.select_one('p.price .amount') or
            soup.select_one('[itemprop="price"]')
        )
        if price:
            t = price.get('content','') or price.get_text()
            pd.price = pd.price or extract_price(t)

        old_price = soup.select_one('p.price del .amount, .woocommerce-Price-amount del')
        if old_price:
            pd.old_price = pd.old_price or extract_price(old_price.get_text())

        # Brand (agar meta tag yoki boshqa joy bo'lsa)
        brand = soup.select_one('[class*="brand"], [itemprop="brand"]')
        if brand:
            pd.brand = pd.brand or clean_text(brand.get_text())

        # SKU
        sku = soup.select_one('.sku, [itemprop="sku"], [class*="sku"]')
        if sku:
            pd.sku = pd.sku or clean_text(sku.get_text())

        # Stock
        stock = soup.select_one('.stock, [class*="availability"]')
        if stock:
            t = clean_text(stock.get_text()).lower()
            if 'в наличии' in t or 'in stock' in t or 'наявн' in t:
                pd.stock_status = 'in_stock'
            elif 'нет' in t or 'out' in t:
                pd.stock_status = 'out_of_stock'

        # Gallery
        gallery = soup.select(
            '.woocommerce-product-gallery__image a, '
            '.product-gallery a[data-src], '
            '.product-images a'
        )
        for a in gallery[:MAX_IMAGES]:
            href = a.get('href','') or a.get('data-src','')
            if href:
                u = abs_url(href, url)
                if u and u not in pd.images:
                    pd.images.append(u)

        # Description
        desc = soup.select_one(
            '[itemprop="description"] p, '
            '.woocommerce-product-details__short-description, '
            '#tab-description'
        )
        if desc:
            pd.description = pd.description or clean_text(desc.get_text())[:2000]

        # Specs
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 9. OpenCart Universal ─────────────────────────────────────────────────────
class OpenCartParser(BaseParser):
    """
    OpenCart asosidagi saytlar uchun universal parser.
    Ishlatiladi: ko'pchilik RU/UA baliqchilik saytlari.
    """
    domains = [
        'fishcomm.ru', 'forum.fishcomm.ru', 'demonfish.ru',
        'fish-chaos.ru', 'fisheroutlet.ru', 'fishing-service.com',
        'fishing1.ru', 'fish-sport.ru', 'fmagazin.ru', 'fspinning.ru',
        'kazanfisher.ru', 'klevaya-ribalka.ru', 'o-n-r.ru',
        'onlyspin.ru', 'perekat24.ru', 'respectactive.com',
        'ribak-ufa.ru', 'riropt.ru', 'russkaja-ohota.ru',
        'rybalka-rt.ru', 'rybalka4you.ru', 'rybalkatop.ru',
        'rybolovmag.ru', 'rybolovnyi.ru', 'rybolovtourist96.ru',
        'rybolov76.ru', 'snastikirov.ru', 'snastimarket.ru',
        'toy3000.ru', 'wmt27.com', 'xakki.by', 'zander-shop.ru',
        'nablesnu.ru', 'fishmarket.pro', 'neva-fish.ru',
        'sl-opt.ru', 'snastikirov.ru',
    ]

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'opencart'

        # Title
        title = soup.select_one(
            '#product h1, h1.product-title, h1[itemprop="name"], '
            '.product-info h1, h1'
        )
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        # Narx
        for price_sel in [
            '#product .price', '.product-info .price', '.price-new',
            '[class*="price-new"]', '[itemprop="price"]',
        ]:
            el = soup.select_one(price_sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        # Eski narx
        old_p = soup.select_one('.price-old, [class*="price-old"]')
        if old_p:
            pd.old_price = pd.old_price or extract_price(old_p.get_text())

        # SKU / model
        for sel in ['[itemprop="sku"]', '.product-info li', '[class*="model"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get_text())
                if t and len(t) < 60:
                    pd.sku = pd.sku or t.split(':')[-1].strip()
                    break

        # Stock
        avail = soup.select_one('[itemprop="availability"], [class*="stock"]')
        if avail:
            t = clean_text(avail.get_text()).lower()
            pd.stock_status = 'in_stock' if 'наличи' in t else (
                'out_of_stock' if 'нет' in t else '')

        # Gallery
        for img_sel in [
            '#product a.thumbnail img', '.product-image a',
            '[class*="thumbnail"] img', '#image',
        ]:
            for img in soup.select(img_sel)[:MAX_IMAGES]:
                parent_a = img.find_parent('a')
                if parent_a:
                    u = abs_url(parent_a.get('href',''), url)
                    if u and u not in pd.images:
                        pd.images.append(u)
                else:
                    src = img.get('src','')
                    if src:
                        pd.images.append(abs_url(src, url))

        # Description
        desc = soup.select_one('#tab-description, [itemprop="description"]')
        if desc:
            pd.description = pd.description or clean_text(desc.get_text())[:2000]

        # Xususiyatlar jadvali
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 10. 1C-Bitrix Universal ───────────────────────────────────────────────────
class BitrixParser(BaseParser):
    """
    1C-Bitrix asosidagi saytlar.
    Ishlatiladi: ko'pchilik RU do'konlari.
    """
    domains = [
        'dikaya-rechka.ru', 'fanat-shop.ru', 'forest-river.ru',
        'hunting-world.ru', 'huntworld.ru', 'krascompass.ru',
        'megafishpro.ru', 'rybaki.ru', 'rybolov76.ru',
        'fishing-land.ru', 'fishingvrn.ru', 'fishing38.ru',
        'allvoblers.ru', 'ebisu66.ru', 'fishingstore.ru',
        'adrenalin.ru', 'snastimarket.ru',
    ]

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'bitrix'

        # Title
        title = soup.select_one(
            'h1[class*="product"], h1[itemprop="name"], '
            '.catalog-element-offer-title, h1'
        )
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        # Narx
        for sel in [
            '.catalog-item-price .price-value', '.product-item-detail-price-actual',
            '[class*="price_value"]', '[class*="product-price"]',
            '.item-price', '[itemprop="price"]',
        ]:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        # Eski narx
        old_p = soup.select_one('[class*="price-old"], [class*="price__old"], del')
        if old_p:
            t = extract_price(old_p.get_text())
            pd.old_price = pd.old_price or t

        # SKU / article
        for sel in ['[class*="article"]', '.catalog-element-sku', '[itemprop="sku"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get_text())
                if re.search(r'[A-Za-z0-9\-]', t) and len(t) < 60:
                    pd.sku = pd.sku or re.sub(r'^[^\w]*', '', t)
                    break

        # Stock
        avail = soup.select_one('[class*="avail"], [class*="in-stock"], [itemprop="availability"]')
        if avail:
            t = clean_text(avail.get_text()).lower()
            pd.stock_status = 'in_stock' if ('наличи' in t or 'есть' in t) else ''

        parse_itemprop_product(soup, pd, url)
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 11. Ratterbaits.com (EN shopify-like) ────────────────────────────────────
class RatterbaitsParser(BaseParser):
    """
    ratterbaits.com — xorijiy baliqchilik do'koni (Shopify asosida).
    JSON-LD bor, lekin Shopify'ning o'z formati ham bor.
    """
    domains = ['ratterbaits.com', 'fishbon.eu']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'shopify_like'

        # Shopify product JSON
        if resp and resp.text:
            m = re.search(r'var meta\s*=\s*(\{[^;]+\})', resp.text)
            if m:
                try:
                    meta = json.loads(m.group(1))
                    prod = meta.get('product', {})
                    pd.title = pd.title or clean_text(prod.get('title', ''))
                    pd.brand = pd.brand or clean_text(prod.get('vendor', ''))
                    variants = prod.get('variants', [{}])
                    if variants:
                        pd.sku = pd.sku or str(variants[0].get('sku', ''))
                        price = variants[0].get('price', 0)
                        pd.price = pd.price or str(int(price) // 100) if price else ''
                    pd.parse_method = 'shopify'
                    return
                except Exception:
                    pass

        title = soup.select_one('h1.product__title, h1.product-title, h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('.price__regular .price-item, .product__price, [class*="price"]')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 12. Rybalkashop.ru (maxsus URL formati) ───────────────────────────────────
class RybalkashopRuParser(BaseParser):
    """
    rybalkashop.ru — maxsus URL: /shop/tagged?id=...&monufacturer=...
    Bu ro'yxat sahifasi, mahsulot sahifasi emas.
    Shuning uchun parse_method='list_page' deb belgilanadi.
    """
    domains = ['rybalkashop.ru']

    def _parse_specific(self, soup, url, resp, pd):
        # URL tekshirish — bu ro'yxat sahifasi
        if 'tagged' in url or 'monufacturer' in url:
            pd.parse_method = 'list_page_skipped'
            pd.extra['note'] = 'Bu URL mahsulot sahifasi emas (ro\'yxat). Mahsulot URL kerak.'
            return

        pd.parse_method = 'rybalkashop'

        title = soup.select_one('h1.product-title, h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('[class*="price"]')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 13. Wildberries list page handler ─────────────────────────────────────────
class WildberriesBrandParser(BaseParser):
    """
    wildberries.ru/brands/... — bu brend ro'yxati sahifasi.
    """
    domains = []  # WildberriesParser bilan to'qnashmaslik uchun

    @classmethod
    def is_list_url(cls, url):
        return bool(re.search(r'wildberries\.ru/brands/', url))


# ── 14. Shop.amurtaimen.ru ─────────────────────────────────────────────────────
class AmurtaimenParser(BaseParser):
    """
    shop.amurtaimen.ru — WooCommerce asosidagi baliqchilik do'koni.
    """
    domains = ['shop.amurtaimen.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'amurtaimen'
        pd.brand = 'Amurtaimen'

        title = soup.select_one('h1.product_title, h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('.woocommerce-Price-amount, .price')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        gallery = soup.select('.woocommerce-product-gallery__image a')
        for a in gallery[:MAX_IMAGES]:
            href = a.get('href','')
            if href:
                pd.images.append(abs_url(href, url))

        pd.specs = pd.specs or extract_specs_table(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 15. Ru.fish ───────────────────────────────────────────────────────────────
class RuFishParser(BaseParser):
    """
    ru.fish — baliqchilik anjomlar kataloги.
    """
    domains = ['ru.fish']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'rufish'

        title = soup.select_one('h1[class*="product"], h1[itemprop="name"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for sel in ['[class*="price-block"]', '.product-price', '[itemprop="price"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        parse_itemprop_product(soup, pd, url)
        pd.specs = pd.specs or extract_specs_table(soup)
        pd.breadcrumbs = pd.breadcrumbs or extract_breadcrumbs(soup)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 16. Fishingstock / Fishing*.ua ────────────────────────────────────────────
class FishingUaParser(BaseParser):
    """
    Ukraina baliqchilik saytlari uchun umumiy parser.
    ibis.net.ua, profish.ua, rybolov-expert.com.ua, rybalka-opt.com.ua
    """
    domains = ['ibis.net.ua']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'fishing_ua'

        title = soup.select_one('h1[itemprop="name"], .product-title, h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for sel in ['.price', '[itemprop="price"]', '[class*="price"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        parse_itemprop_product(soup, pd, url)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 17. Megafish.by ────────────────────────────────────────────────────────────
class MegafishByParser(BaseParser):
    domains = ['megafish.by']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'megafish_by'

        title = soup.select_one('h1.product__name, h1[class*="product"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('.product__price, [class*="price"]')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        gallery = soup.select('.product__image a, .product-gallery a')
        for a in gallery[:MAX_IMAGES]:
            href = a.get('href','')
            if href:
                pd.images.append(abs_url(href, url))

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 18. Papafish.by ────────────────────────────────────────────────────────────
class PapafishParser(BaseParser):
    domains = ['papafish.by']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'papafish'

        title = soup.select_one('h1.product_title, h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        price = soup.select_one('.woocommerce-Price-amount, .price')
        if price:
            pd.price = pd.price or extract_price(price.get_text())

        gallery = soup.select('.woocommerce-product-gallery__image a')
        for a in gallery[:MAX_IMAGES]:
            href = a.get('href','')
            if href:
                pd.images.append(abs_url(href, url))

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 19. Fishingvrn.ru ──────────────────────────────────────────────────────────
class FishingvrnParser(BaseParser):
    domains = ['fishingvrn.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'fishingvrn'

        # FishingVRN — Bitrix asosida
        title = soup.select_one('[class*="product-name"], h1')
        if title:
            pd.title = pd.title or clean_text(title.get_text())

        for sel in ['[class*="price"]', '[itemprop="price"]']:
            el = soup.select_one(sel)
            if el:
                t = clean_text(el.get('content','') or el.get_text())
                if re.search(r'\d{2,}', t):
                    pd.price = pd.price or extract_price(t)
                    break

        # URL dan SKU
        m = re.search(r'-(\w{5,})$', url.rstrip('/'))
        if m:
            pd.sku = pd.sku or m.group(1)

        parse_itemprop_product(soup, pd, url)

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ── 20. AliExpress.ru (to'g'rilash) ──────────────────────────────────────────
class AliexpressRuParser(BaseParser):
    """
    aliexpress.ru — AliexpressParser bilan bir xil, lekin .ru domeni uchun.
    """
    domains = ['aliexpress.ru']

    def _parse_specific(self, soup, url, resp, pd):
        pd.parse_method = 'aliexpress_ru'

        # AE JS state
        if resp:
            for pattern in [
                r'window\.runParams\s*=\s*(\{.+?\});\s*</script>',
                r'"productInfoComponent":\s*(\{[^}]+\})',
                r'window\.__aer_data__\s*=\s*(\{.+?\})\s*;',
            ]:
                m = re.search(pattern, resp.text or '', re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        comp = data.get('productInfoComponent', data)
                        pd.title = pd.title or comp.get('subject', '')
                        price_comp = data.get('priceComponent', {})
                        p = price_comp.get('discountPrice', {}).get('value', '')
                        pd.price = pd.price or extract_price(str(p))
                        break
                    except Exception:
                        pass

        if not pd.title:
            h1 = soup.find('h1')
            if h1:
                pd.title = clean_text(h1.get_text())

        if not pd.images:
            pd.images = self.find_images_generic(soup, url)


# ═══════════════════════════════════════════════════════════════════════════════
#  PARSER REGISTRY (KENGAYTIRILGAN)
# ═══════════════════════════════════════════════════════════════════════════════

EXTENDED_PARSERS = [
    AliexpressRuParser(),
    SimaLandParser(),
    MeshokParser(),
    HftParser(),
    BiggameParser(),
    TrophyFishingParser(),
    OlympsportParser(),
    ReviewsYandexParser(),
    AmurtaimenParser(),
    RuFishParser(),
    MegafishByParser(),
    PapafishParser(),
    FishingvrnParser(),
    WooCommerceParser(),
    OpenCartParser(),
    BitrixParser(),
    RatterbaitsParser(),
    RybalkashopRuParser(),
    FishingUaParser(),
]

# Domen → parser mapping
EXTENDED_DOMAIN_MAP = {}
for _p in EXTENDED_PARSERS:
    for _d in _p.domains:
        EXTENDED_DOMAIN_MAP[_d] = _p


def get_extended_parser(domain: str):
    """
    Kengaytirilgan parser ro'yxatidan mos parserni qaytaradi.
    Agar topilmasa None qaytaradi (step6_scraper.get_parser() ga uzatish uchun).
    """
    if domain in EXTENDED_DOMAIN_MAP:
        return EXTENDED_DOMAIN_MAP[domain]
    # Subdomain tekshirish
    parts = domain.split('.')
    for i in range(1, len(parts)):
        parent = '.'.join(parts[i:])
        if parent in EXTENDED_DOMAIN_MAP:
            return EXTENDED_DOMAIN_MAP[parent]
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  URL VALIDATOR — ro'yxat sahifalarini filtrlash
# ═══════════════════════════════════════════════════════════════════════════════

LIST_PAGE_PATTERNS = [
    r'/brands?/',           # wildberries.ru/brands/kosadaka/
    r'[?&]tagged=',         # rybalkashop.ru/shop/tagged?...
    r'[?&]monufacturer=',   # rybalkashop.ru filter
    r'[?&]sort=',           # ro'yxat sort parametri
    r'/catalog/?$',         # katalog sahifasi
    r'/search[/?]',         # qidiruv sahifasi
    r'/reviews/?$',         # sharhlar sahifasi (ozon.by)
    r'/category/',          # kategoriya
    r'/tag/',               # tag sahifasi
]

PRODUCT_PAGE_HINTS = [
    r'/product/', r'/products/', r'/item/', r'/detail', r'/dp/',
    r'/p\d{4,}', r'\d{6,}', r'/catalog/\w+/\w+/\w+',
]


def is_list_page(url: str) -> bool:
    """URL ro'yxat sahifasi ekanligini tekshiradi."""
    for pattern in LIST_PAGE_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


def is_product_page(url: str) -> bool:
    """URL mahsulot sahifasi ekanligini taxmin qiladi."""
    for pattern in PRODUCT_PAGE_HINTS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  INTEGRATSIYA: step6_scraper bilan bog'lash
# ═══════════════════════════════════════════════════════════════════════════════

def patch_step6_get_parser():
    """
    step6_scraper.get_parser() funksiyasini kengaytirilgan parser bilan patch qiladi.
    Ishlatish: parsers_extended.patch_step6_get_parser() ni import qilib chaqiring.
    """
    try:
        import step6_scraper as s6

        original_get_parser = s6.get_parser

        def new_get_parser(domain: str):
            # Avval kengaytirilgan parser qidirish
            ext = get_extended_parser(domain)
            if ext:
                return ext
            # Keyin original
            return original_get_parser(domain)

        s6.get_parser = new_get_parser
        # EXTENDED_DOMAIN_MAP ni s6._DOMAIN_MAP ga qo'shish
        s6._DOMAIN_MAP.update(EXTENDED_DOMAIN_MAP)
        print(f"[parsers_extended] Patch muvaffaqiyatli: "
              f"{len(EXTENDED_DOMAIN_MAP)} ta yangi domen qo'shildi.")
        return True
    except ImportError:
        print("[parsers_extended] step6_scraper topilmadi, standalone rejimda ishlaydi.")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  BARCHA DOMENLAR RO'YXATI (jsonlar.txt dan)
# ═══════════════════════════════════════════════════════════════════════════════

ALL_DOMAINS_FROM_JSON = {
    # --- Maxsus parser BILAN (step6_scraper da) ---
    "wildberries.ru":        "WildberriesParser (API)",
    "global.wildberries.ru": "WildberriesParser (API)",
    "ozon.ru":               "OzonParser (JS state)",
    "ozon.by":               "OzonParser (JS state) — /reviews/ URL, mahsulot sahifasi emas!",
    "ozon.kz":               "OzonParser (JS state)",
    "am.ozon.com":           "OzonParser (JS state)",
    "uz.ozon.com":           "OzonParser (JS state)",
    "market.yandex.ru":      "YandexMarketParser",
    "market.yandex.uz":      "YandexMarketParser",
    "avito.ru":              "AvitoParser",
    "m.avito.ru":            "AvitoParser",
    "megamarket.ru":         "MegamarketParser",
    "sbermegamarket.ru":     "MegamarketParser",
    "amazon.com":            "AmazonParser (API)",
    "aliexpress.com":        "AliexpressParser",
    "aliexpress.ru":         "AliexpressRuParser (YANGI)",
    "rozetka.com.ua":        "RozetkaParser",
    "prom.ua":               "PromUaParser",
    "kosadakashop.ru":       "KosadakaShopParser",

    # --- Kengaytirilgan parserlar (YANGI) ---
    "sima-land.ru":          "SimaLandParser (YANGI)",
    "meshok.net":            "MeshokParser (YANGI)",
    "hft.ru":                "HftParser/BitrixParser (YANGI)",
    "biggame.ru":            "BiggameParser (YANGI)",
    "trophyfishing.ru":      "TrophyFishingParser (YANGI)",
    "krasnoyarsk.trophyfishing.ru": "TrophyFishingParser (YANGI)",
    "ufa.trophyfishing.ru":  "TrophyFishingParser (YANGI)",
    "volgograd.trophyfishing.ru": "TrophyFishingParser (YANGI)",
    "olympsport.spb.ru":     "OlympsportParser (YANGI)",
    "reviews.yandex.ru":     "ReviewsYandexParser (YANGI, sharhlar sahifasi)",
    "shop.amurtaimen.ru":    "AmurtaimenParser/WooCommerce (YANGI)",
    "ru.fish":               "RuFishParser (YANGI)",
    "megafish.by":           "MegafishByParser (YANGI)",
    "papafish.by":           "PapafishParser (YANGI)",
    "fishingvrn.ru":         "FishingvrnParser (YANGI)",
    "rybalkashop.ru":        "RybalkashopRuParser — ro'yxat URL, mahsulot sahifasi emas!",

    # --- WooCommerce (YANGI) ---
    "fish-rod.com.ua":       "WooCommerceParser (YANGI)",
    "spinning.by":           "WooCommerceParser (YANGI)",
    "pikemaster.com.ua":     "WooCommerceParser (YANGI)",
    "rybalkashop.by":        "WooCommerceParser (YANGI)",
    "vobler.by":             "WooCommerceParser (YANGI)",
    "kuldkalake.eu":         "WooCommerceParser (YANGI)",
    "triturus-fishing.com":  "WooCommerceParser (YANGI)",
    "synthetic.ua":          "WooCommerceParser (YANGI)",
    "fishbon.eu":            "WooCommerceParser (YANGI)",
    "rybalka-opt.com.ua":    "WooCommerceParser (YANGI)",
    "profish.ua":            "WooCommerceParser (YANGI)",
    "rybolov-expert.com.ua": "WooCommerceParser (YANGI)",
    "shimano.kiev.ua":       "WooCommerceParser (YANGI)",
    "ibis-gear.com":         "WooCommerceParser (YANGI)",
    "wobblershop.ru":        "WooCommerceParser (YANGI)",
    "spinningline.ru":       "WooCommerceParser (YANGI)",
    "fishingstock.ua":       "WooCommerceParser (YANGI)",
    "xn----ctbibbzhmpe6am.com": "WooCommerceParser (YANGI)",

    # --- OpenCart (YANGI) ---
    "fishcomm.ru":           "OpenCartParser (YANGI)",
    "forum.fishcomm.ru":     "OpenCartParser (YANGI)",
    "demonfish.ru":          "OpenCartParser (YANGI)",
    "fish-chaos.ru":         "OpenCartParser (YANGI)",
    "fisheroutlet.ru":       "OpenCartParser (YANGI)",
    "fishing-service.com":   "OpenCartParser (YANGI)",
    "fishing1.ru":           "OpenCartParser (YANGI)",
    "fish-sport.ru":         "OpenCartParser (YANGI)",
    "fmagazin.ru":           "OpenCartParser (YANGI)",
    "fspinning.ru":          "OpenCartParser (YANGI)",
    "kazanfisher.ru":        "OpenCartParser (YANGI)",
    "klevaya-ribalka.ru":    "OpenCartParser (YANGI)",
    "o-n-r.ru":              "OpenCartParser (YANGI)",
    "onlyspin.ru":           "OpenCartParser (YANGI)",
    "perekat24.ru":          "OpenCartParser (YANGI)",
    "resprespectactive.com": "OpenCartParser (YANGI)",
    "ribak-ufa.ru":          "OpenCartParser (YANGI)",
    "riropt.ru":             "OpenCartParser (YANGI)",
    "russkaja-ohota.ru":     "OpenCartParser (YANGI)",
    "rybalka-rt.ru":         "OpenCartParser (YANGI)",
    "rybalka4you.ru":        "OpenCartParser (YANGI)",
    "rybalkatop.ru":         "OpenCartParser (YANGI)",
    "rybolovmag.ru":         "OpenCartParser (YANGI)",
    "rybolovnyi.ru":         "OpenCartParser (YANGI)",
    "rybolovtourist96.ru":   "OpenCartParser (YANGI)",
    "rybolov76.ru":          "OpenCartParser (YANGI)",
    "snastikirov.ru":        "OpenCartParser (YANGI)",
    "snastimarket.ru":       "OpenCartParser (YANGI)",
    "toy3000.ru":            "OpenCartParser (YANGI)",
    "wmt27.com":             "OpenCartParser (YANGI)",
    "xakki.by":              "OpenCartParser (YANGI)",
    "zander-shop.ru":        "OpenCartParser (YANGI)",
    "nablesnu.ru":           "OpenCartParser (YANGI)",
    "fishmarket.pro":        "OpenCartParser (YANGI)",
    "neva-fish.ru":          "OpenCartParser (YANGI)",
    "sl-opt.ru":             "OpenCartParser (YANGI)",

    # --- 1C-Bitrix (YANGI) ---
    "dikaya-rechka.ru":      "BitrixParser (YANGI)",
    "fanat-shop.ru":         "BitrixParser (YANGI)",
    "forest-river.ru":       "BitrixParser (YANGI)",
    "huntworld.ru":          "BitrixParser (YANGI)",
    "krascompass.ru":        "BitrixParser (YANGI)",
    "megafishpro.ru":        "BitrixParser (YANGI)",
    "rybaki.ru":             "BitrixParser (YANGI)",
    "fishing-land.ru":       "BitrixParser (YANGI)",
    "allvoblers.ru":         "BitrixParser (YANGI)",
    "ebisu66.ru":            "BitrixParser (YANGI)",
    "fishingstore.ru":       "BitrixParser (YANGI)",
    "adrenalin.ru":          "BitrixParser (YANGI)",
    "fishing38.ru":          "BitrixParser (YANGI)",

    # --- Shopify-like (YANGI) ---
    "ratterbaits.com":       "RatterbaitsParser/Shopify (YANGI)",

    # --- FishingStoreParser / UniversalParser (mavjud) ---
    "59rost.ru":             "FishingStoreParser (mavjud) — subdom: dobryanka.59rost.ru",
    "dobryanka.59rost.ru":   "FishingStoreParser — 59rost.ru subdom orqali (mavjud)",
    "caiman-fishing.ru":     "WooCommerceParser (YANGI)",
    "caimanfishing.ru":      "WooCommerceParser (YANGI)",
    "fish-chaos.ru":         "OpenCartParser (YANGI)",
    "ibis.net.ua":           "FishingUaParser (YANGI)",
    "fishingstock.ua":       "WooCommerceParser (YANGI)",
}


def print_coverage_report():
    """Barcha domenlar uchun qamrov hisobotini chiqaradi."""
    print("\n" + "="*70)
    print("  DOMEN QAMROVI HISOBOTI")
    print("="*70)

    covered = []
    problem_urls = []
    universal = []

    for domain, parser_info in ALL_DOMAINS_FROM_JSON.items():
        if 'ro\'yxat' in parser_info or 'mahsulot sahifasi emas' in parser_info:
            problem_urls.append((domain, parser_info))
        elif 'YANGI' in parser_info or 'mavjud' in parser_info or 'API' in parser_info:
            covered.append((domain, parser_info))
        else:
            universal.append((domain, parser_info))

    print(f"\n✅ MAXSUS PARSER BILAN: {len(covered)} ta")
    for d, p in covered[:10]:
        print(f"   {d:<35} → {p}")
    if len(covered) > 10:
        print(f"   ... va yana {len(covered)-10} ta")

    print(f"\n⚠️  MUAMMOLI URLlar: {len(problem_urls)} ta")
    for d, p in problem_urls:
        print(f"   {d:<35} → {p}")

    print(f"\n📊 Jami domenlar: {len(ALL_DOMAINS_FROM_JSON)}")
    print("="*70 + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT — step6_scraper bilan integratsiya yoki standalone test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys

    print_coverage_report()

    if '--patch' in sys.argv:
        patch_step6_get_parser()

    elif '--test' in sys.argv and len(sys.argv) > 2:
        # python parsers_extended.py --test https://example.com
        test_url = sys.argv[sys.argv.index('--test') + 1]
        domain = re.sub(r'^www\.', '', urlparse(test_url).netloc.lower())
        parser = get_extended_parser(domain)
        if parser is None:
            print(f"Kengaytirilgan parser topilmadi: {domain}")
            print("UniversalParser ishlatiladi.")
        else:
            print(f"Parser: {parser.__class__.__name__}")
            resp = fetch_url(test_url)
            if resp:
                soup = BeautifulSoup(resp.content, 'lxml')
                pd = parser.parse(soup, test_url, resp)
                print(f"  title  : {pd.title}")
                print(f"  brand  : {pd.brand}")
                print(f"  price  : {pd.price}")
                print(f"  sku    : {pd.sku}")
                print(f"  images : {len(pd.images)} ta")
                print(f"  specs  : {len(pd.specs)} ta")
                print(f"  method : {pd.parse_method}")
            else:
                print("Sahifa yuklanmadi.")
    else:
        print("\nFoydalanish:")
        print("  python parsers_extended.py                   # hisobot")
        print("  python parsers_extended.py --patch           # step6 bilan integratsiya")
        print("  python parsers_extended.py --test <URL>      # test")
        print("\nStep6 bilan ishlatish:")
        print("  # step6_run.py faylida:")
        print("  import parsers_extended")
        print("  parsers_extended.patch_step6_get_parser()")
        print("  # Keyin step6_scraper.run_scraper() ni chaqiring")
