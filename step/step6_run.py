"""
step6_run.py — step6_scraper + parsers_extended birgalikda ishlatish

Ishlatish:
  python step6_run.py                    # to'liq scraping
  python step6_run.py --domain ozon.ru   # bitta domen
  python step6_run.py --limit 50         # faqat 50 URL
    python step6_run.py --notify-telegram  # Telegram progress + batch jo'natish
  python step6_run.py --stats            # holat
  python step6_run.py --check-urls       # URL muammolarini ko'rsatish
"""

import sys

# 1. Kengaytirilgan parserlarni yuklash va step6 ga patch qilish
import parsers_extended
parsers_extended.patch_step6_get_parser()

# 2. URL muammo tekshiruvi
import step6_scraper as s6
import sqlite3

def check_problematic_urls():
    """Ro'yxat sahifalari va boshqa muammoli URLlarni ko'rsatadi."""
    if not __import__('os').path.exists(s6.DST_DB):
        print("scraped.db topilmadi. Avval --init bajaring.")
        return

    conn = sqlite3.connect(s6.DST_DB)
    urls = conn.execute("SELECT url, domain FROM scrape_queue WHERE status='pending'").fetchall()
    conn.close()

    problems = []
    for url, domain in urls:
        if parsers_extended.is_list_page(url):
            problems.append((domain, url, "RO'YXAT SAHIFASI"))

    if problems:
        print(f"\n⚠️  {len(problems)} ta muammoli URL topildi:\n")
        for dom, url, reason in problems[:20]:
            print(f"  [{reason}] {dom}")
            print(f"    {url[:80]}")
        if len(problems) > 20:
            print(f"  ... va yana {len(problems)-20} ta")
    else:
        print("✅ Muammoli URL topilmadi.")


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--check-urls' in args:
        check_problematic_urls()
        sys.exit(0)

    if '--coverage' in args:
        parsers_extended.print_coverage_report()
        sys.exit(0)

    # Qolgan barcha argumentlarni step6_scraper ga uzatish
    domain_filter = None
    limit_n = None
    workers_n = s6.MAX_WORKERS
    no_images = False
    notify_telegram = False
    tg_batch_size = s6.TELEGRAM_BATCH_SIZE

    if '--domain' in args:
        i = args.index('--domain')
        domain_filter = args[i + 1]

    if '--limit' in args:
        i = args.index('--limit')
        limit_n = int(args[i + 1])

    if '--workers' in args:
        i = args.index('--workers')
        workers_n = int(args[i + 1])

    if '--no-images' in args:
        no_images = True

    if '--notify-telegram' in args:
        notify_telegram = True

    if '--tg-batch-size' in args:
        i = args.index('--tg-batch-size')
        tg_batch_size = max(1, int(args[i + 1]))

    if '--stats' in args:
        s6.show_stats()
    elif '--retry-errors' in args:
        conn = sqlite3.connect(s6.DST_DB)
        conn.execute("UPDATE scrape_queue SET status='pending', attempt=0 WHERE status='error'")
        conn.commit()
        cnt = conn.execute("SELECT changes()").fetchone()[0]
        conn.close()
        print(f"{cnt} error → pending qilindi")
        s6.run_scraper(domain_filter, limit_n, workers_n, no_images, notify_telegram, tg_batch_size)
    elif '--init' in args:
        s6.init_scraped_db()
        s6.load_queue_from_pipeline(domain_filter=domain_filter)
    else:
        s6.run_scraper(domain_filter, limit_n, workers_n, no_images, notify_telegram, tg_batch_size)
