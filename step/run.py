"""
run.py — to'liq pipeline boshqaruvi

Ishlatish:
  python run.py            → to'liq pipeline (step1 + step2 + step3)
  python run.py --prepare  → faqat step1+step2 (JSON tayyorlash + DB yuklash)
  python run.py --search   → faqat step3 (qidiruvni davom ettirish)
  python run.py --filter   → faqat step4 (URL filtrlash → clean_products)
  python run.py --status   → joriy holat statistikasi
  python run.py --export   → natijalarni JSON ga eksport qilish
"""

import sys
import os
import json
import sqlite3

BASE = os.path.dirname(os.path.abspath(__file__))


def cmd_prepare():
    from step1_prepare import prepare_queries
    from step2_db       import init_db, load_queries_to_db

    q_path = os.path.join(BASE, '../search_queries.json')
    prepare_queries(
        brands_json_path = os.path.join(BASE, '../brands.json'),
        output_path      = q_path
    )
    init_db()
    load_queries_to_db(q_path)
    print("\n[run.py] Tayyorlov tugadi. Qidiruv uchun: python run.py --search")


def cmd_search():
    from step3_search import run_pipeline
    run_pipeline()


def cmd_filter():
    from step4_filter import run_filter, show_stats
    run_filter()
    show_stats()


def cmd_status():
    db = os.path.join(BASE, '../pipeline.db')
    if not os.path.exists(db):
        print("DB topilmadi. Avval: python run.py --prepare")
        return
    conn = sqlite3.connect(db)
    cur  = conn.execute("""
        SELECT status, COUNT(*) as cnt FROM search_tasks GROUP BY status
    """)
    rows  = cur.fetchall()
    total = sum(r[1] for r in rows)
    print(f"\n{'─'*35}")
    print(f"  Pipeline holati (jami: {total})")
    print(f"{'─'*35}")
    for status, cnt in rows:
        bar = '█' * (cnt * 30 // max(total, 1))
        print(f"  {status:<12} {cnt:>5}  {bar}")
    print(f"{'─'*35}")

    cur2 = conn.execute("SELECT COUNT(*) FROM search_results")
    print(f"  URLs saqlangan: {cur2.fetchone()[0]}")

    # clean_products mavjud bo'lsa ko'rsat
    try:
        cur3 = conn.execute("SELECT COUNT(*) FROM clean_products")
        print(f"  Filtered URLs : {cur3.fetchone()[0]}")
    except Exception:
        pass

    print(f"{'─'*35}\n")
    conn.close()


def cmd_export():
    """Natijalarni brand->product->urls ko'rinishida JSON ga chiqaradi."""
    db = os.path.join(BASE, '../pipeline.db')
    if not os.path.exists(db):
        print("DB topilmadi.")
        return

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    cur = conn.execute("""
        SELECT
            t.brand,
            t.product_id,
            t.sku,
            t.barcode,
            t.text1,
            t.query_used,
            r.url,
            r.title,
            r.position
        FROM search_results r
        JOIN search_tasks t ON r.task_id = t.id
        ORDER BY t.brand, t.product_id, r.position
    """)

    export = {}
    for row in cur.fetchall():
        brand = row['brand']
        pid   = row['product_id']

        if brand not in export:
            export[brand] = {}
        if pid not in export[brand]:
            export[brand][pid] = {
                "sku":      row['sku'],
                "barcode":  row['barcode'],
                "name":     row['text1'],
                "query":    row['query_used'],
                "urls":     []
            }
        export[brand][pid]['urls'].append({
            "position": row['position'],
            "url":      row['url'],
            "title":    row['title']
        })

    out = os.path.join(BASE, 'results_export.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    total_urls = sum(
        len(p['urls'])
        for brand in export.values()
        for p in brand.values()
    )
    print(f"[export] {len(export)} brend, {total_urls} URL → {out}")
    conn.close()


def cmd_export_clean():
    """clean_products dan keyingi loyiha uchun JSON eksport."""
    db = os.path.join(BASE, '../pipeline.db')
    if not os.path.exists(db):
        print("DB topilmadi.")
        return

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.execute("""
            SELECT brand, barcode, sku, product_id, product_name,
                   url, url_domain, filter_score
            FROM clean_products
            ORDER BY brand, barcode, filter_score DESC
        """)
    except Exception:
        print("clean_products topilmadi. Avval: python run.py --filter")
        conn.close()
        return

    export = {}
    for row in cur.fetchall():
        brand   = row['brand']
        barcode = row['barcode']

        if brand not in export:
            export[brand] = {}
        if barcode not in export[brand]:
            export[brand][barcode] = {
                "sku":          row['sku'],
                "product_id":   row['product_id'],
                "product_name": row['product_name'],
                "urls":         []
            }
        export[brand][barcode]['urls'].append({
            "url":    row['url'],
            "domain": row['url_domain'],
            "score":  row['filter_score'],
        })

    out = os.path.join(BASE, 'clean_export.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    total = sum(
        len(p['urls'])
        for brand in export.values()
        for p in brand.values()
    )
    products = sum(len(brand) for brand in export.values())
    print(f"[clean_export] {len(export)} brend, {products} mahsulot, {total} URL → {out}")
    conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    args = sys.argv[1:]

    if not args or args[0] == '--all':
        cmd_prepare()
        print("\nQidiruv boshlanyapti...\n")
        cmd_search()

    elif args[0] == '--prepare':
        cmd_prepare()

    elif args[0] == '--search':
        cmd_search()

    elif args[0] == '--filter':
        cmd_filter()

    elif args[0] == '--status':
        cmd_status()

    elif args[0] == '--export':
        cmd_export()

    elif args[0] == '--export-clean':
        cmd_export_clean()

    else:
        print(__doc__)