"""
STEP 1: brands.json -> search_queries.json
Har bir mahsulot uchun text1 (asl nom) va text2 (ruscha so'zlarsiz) tayyorlaydi.
"""

import json
import re
import os

# ── Ruscha so'zlarni olib tashlash ──────────────────────────────────────────
RUSSIAN_PATTERN = re.compile(r'[А-ЯЁа-яё]+', re.UNICODE)

# Mahsulot nomidan brand nomini olib tashlash (boshidan)
def strip_brand_prefix(name: str, brand: str) -> str:
    """Nom boshidan brand nomini olib tashlaydi (case-insensitive)."""
    brand_clean = brand.strip().upper()
    name_upper  = name.strip().upper()
    if name_upper.startswith(brand_clean):
        name = name[len(brand_clean):].strip(" -_,")
    return name.strip()


def make_text2(name: str, brand: str) -> str:
    """
    text2: ruscha so'zlarni olib tashlab, faqat lotincha/raqam/belgi qoldiradi.
    Agar hamma narsa ruscha bo'lsa – brand + SKU-like qism saqlanadi.
    """
    # Brand prefixni olib tashla
    cleaned = strip_brand_prefix(name, brand)

    # Ruscha so'zlarni bo'sh joyga almashtir
    no_ru = RUSSIAN_PATTERN.sub(' ', cleaned)

    # Ketma-ket bo'shliqlarni tozala
    no_ru = re.sub(r'\s+', ' ', no_ru).strip()

    # Agar deyarli bo'sh qoldi (< 4 belgi) – asl nomdan raqam+lotin qismlarini qol
    if len(no_ru) < 4:
        tokens = re.findall(r'[A-Za-z0-9][A-Za-z0-9\-\.]*', name)
        no_ru = ' '.join(tokens).strip()

    # Hali ham bo'sh? – brand + asl nom (lotincha qism)
    if not no_ru:
        latin_tokens = re.findall(r'[A-Za-z0-9][A-Za-z0-9\-\.]*', name)
        no_ru = brand + ' ' + ' '.join(latin_tokens)

    return no_ru.strip()


def prepare_queries(brands_json_path: str, output_path: str):
    with open(brands_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    result = {}
    total  = 0

    for brand, products in data.items():
        brand_list = []
        for product in products:
            raw_name = product.get('name', '').strip()
            if not raw_name:
                continue

            text1 = raw_name                        # Asl to'liq nom
            text2 = make_text2(raw_name, brand)     # Ruscha so'zlarsiz

            # Agar text1 == text2 (butunlay lotin edi) – text2 ni None qil
            if text1.strip() == text2.strip():
                text2 = None

            brand_list.append({
                "id":       product.get('id', ''),
                "sku":      product.get('sku', ''),
                "barcode":  product.get('barcode', ''),
                "text1":    text1,
                "text2":    text2,
                "status":   "pending"   # pipeline uchun
            })
            total += 1

        if brand_list:
            result[brand] = brand_list

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[STEP1] Tayyor: {len(result)} brend, {total} mahsulot → {output_path}")
    return total


if __name__ == '__main__':
    BASE = os.path.dirname(os.path.abspath(__file__))
    prepare_queries(
        brands_json_path = os.path.join(BASE, '../brands.json'),
        output_path      = os.path.join(BASE, '../search_queries.json')
    )
