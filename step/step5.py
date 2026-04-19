"""
step5_unique_domains.py

Maqsad:
  clean_products tablidan barcha domenlarni topib,
  har biridan BITTA namunaviy havola olib JSON ga yozadi.

  Natija: saytlarni o'rganish uchun — qaysi saytlar bor
  va ularning havolalari qanday ko'rinishda ekanligini bilish.

Ishlatish:
  python step5_unique_domains.py
"""

import sqlite3
import json
import os
import sys
import re
from urllib.parse import urlparse

BASE     = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE, '../pipeline.db')
OUT_PATH = os.path.join(BASE, '../unique_domains.json')


def normalize_domain(netloc: str) -> str:
    return re.sub(r'^www\.', '', netloc.lower().strip())


def get_domain(url: str) -> str:
    try:
        return normalize_domain(urlparse(url).netloc)
    except Exception:
        return ''


def run(db_path: str = DB_PATH, out_path: str = OUT_PATH):
    if not os.path.exists(db_path):
        print(f"[XATO] pipeline.db topilmadi: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)

    # Har domendan eng yuqori scored bitta URL
    rows = conn.execute("""
        SELECT url_domain, url, MAX(filter_score) as score
        FROM clean_products
        WHERE url IS NOT NULL AND url != ''
        GROUP BY url_domain
        ORDER BY url_domain
    """).fetchall()

    conn.close()

    # { domain: url } ko'rinishida yig'
    result = {}
    for url_domain, url, score in rows:
        domain = url_domain or get_domain(url) or 'unknown'
        result[domain] = url

    # JSON ga yoz
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\nJami unique saytlar : {len(result)}")
    print(f"Saqlandi            : {out_path}\n")

    # Konsolda ro'yxatni ham ko'rsat
    for domain, url in sorted(result.items()):
        print(f"  {domain:<30}  {url}")


if __name__ == '__main__':
    args = sys.argv[1:]
    db_path  = DB_PATH
    out_path = OUT_PATH

    if '--db' in args:
        idx = args.index('--db')
        db_path = args[idx + 1]

    if '--out' in args:
        idx = args.index('--out')
        out_path = args[idx + 1]

    run(db_path, out_path)