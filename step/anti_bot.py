import json
import logging
import os
import random
import time
from itertools import cycle
from typing import Optional

import requests

log = logging.getLogger(__name__)

_PROXIES_RAW = os.getenv("ANTIBOT_PROXIES", "")
_PROXIES = [p.strip() for p in _PROXIES_RAW.split(",") if p.strip()]
_PROXY_CYCLE = cycle(_PROXIES) if _PROXIES else None

DELAY_MIN = float(os.getenv("ANTIBOT_DELAY_MIN", "2.0"))
DELAY_MAX = float(os.getenv("ANTIBOT_DELAY_MAX", "6.0"))
CAPTCHA_API_KEY = os.getenv("ANTIBOT_2CAPTCHA_KEY", "").strip()
STEALTH_ENABLED = os.getenv("ANTIBOT_STEALTH_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
]


def get_next_proxy_url() -> Optional[str]:
    if not _PROXY_CYCLE:
        return None
    return next(_PROXY_CYCLE)


def get_proxy_dict() -> Optional[dict]:
    proxy = get_next_proxy_url()
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def random_browser_headers(extra: Optional[dict] = None) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(["ru-RU,ru;q=0.9,en-US;q=0.8", "en-US,en;q=0.9"]),
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }
    if extra:
        headers.update(extra)
    return headers


def human_sleep(min_sec: Optional[float] = None, max_sec: Optional[float] = None) -> None:
    min_v = DELAY_MIN if min_sec is None else min_sec
    max_v = DELAY_MAX if max_sec is None else max_sec
    if max_v < min_v:
        max_v = min_v
    time.sleep(random.uniform(min_v, max_v))


def detect_captcha_or_block(status_code: int, body_text: str) -> tuple[bool, str]:
    if status_code in (403, 429, 503, 520, 521, 522, 523, 524):
        return True, f"http_{status_code}"

    txt = (body_text or "").lower()
    markers = [
        "captcha",
        "recaptcha",
        "hcaptcha",
        "cf-chl",
        "cloudflare",
        "access denied",
        "robot",
        "too many requests",
        "blocked",
    ]
    for m in markers:
        if m in txt:
            return True, m

    return False, ""


def solve_captcha_2captcha(site_key: str, page_url: str) -> Optional[str]:
    if not CAPTCHA_API_KEY:
        return None

    try:
        payload = {
            "key": CAPTCHA_API_KEY,
            "method": "userrecaptcha",
            "googlekey": site_key,
            "pageurl": page_url,
            "json": 1,
        }
        r = requests.post("http://2captcha.com/in.php", data=payload, timeout=30)
        data = r.json()
        if data.get("status") != 1:
            return None

        captcha_id = data.get("request")
        for _ in range(30):
            time.sleep(3)
            rr = requests.get(
                "http://2captcha.com/res.php",
                params={"key": CAPTCHA_API_KEY, "action": "get", "id": captcha_id, "json": 1},
                timeout=15,
            )
            d = rr.json()
            if d.get("status") == 1:
                return d.get("request")
            if d.get("request") != "CAPCHA_NOT_READY":
                return None
    except Exception as e:
        log.warning(f"2captcha solve xato: {e}")

    return None


def playwright_stealth_fetch(
    url: str,
    proxy_url: Optional[str] = None,
    timeout_ms: int = 45000,
) -> Optional[dict]:
    if not STEALTH_ENABLED:
        return None

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None

    try:
        with sync_playwright() as p:
            launch_kwargs = {"headless": True}
            if proxy_url:
                launch_kwargs["proxy"] = {"server": proxy_url}

            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(user_agent=random.choice(USER_AGENTS))
            context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                window.chrome = { runtime: {} };
                """
            )
            page = context.new_page()
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(700)
            html = page.content()
            status = resp.status if resp else 200
            headers = resp.headers if resp else {}
            context.close()
            browser.close()
            return {"status_code": status, "content": html, "headers": headers, "url": page.url}
    except Exception as e:
        log.warning(f"stealth fetch xato: {e}")
        return None


class SimpleResponse:
    """Fallback response wrapper for browser-based HTML fetch."""

    def __init__(self, status_code: int, headers: dict, text: str):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text or ""
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"
        self.content = self.text.encode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.text)
