# Scrapy settings for bse_scraper project

import os
from pathlib import Path

BOT_NAME = "bse_scraper"

SPIDER_MODULES = ["bse_scraper.spiders"]
NEWSPIDER_MODULE = "bse_scraper.spiders"

# --- Core behavior ---
ROBOTSTXT_OBEY = False
DOWNLOAD_TIMEOUT = 60
RETRY_TIMES = 2
LOG_LEVEL = "INFO"  # or "DEBUG" while debugging

# Twisted reactor (future-proof)
# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
TWISTED_REACTOR = "twisted.internet.selectreactor.SelectReactor"


# --- FilesPipeline local cache (before we upload to R2 in the pipeline) ---
BASE_DIR = Path(__file__).resolve().parent            # .../bse_scraper
PROJECT_ROOT = BASE_DIR.parent                        # D:/BSE_django/bse_api
FILES_STORE = str(PROJECT_ROOT / "downloads")

# Sometimes BSE PDF links redirect; allow media redirects
MEDIA_ALLOW_REDIRECTS = True

# --- Feed exports (optional) ---
# If you are passing "-O outputs/announcements.json" from the DRF view,
# you can COMMENT OUT the FEEDS block below to avoid duplicate files.
OUT_DIR = PROJECT_ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FEED_EXPORT_ENCODING = "utf-8"
FEEDS = {
    # Timestamped JSON array file per run
    str(OUT_DIR / "announcements-%(time)s.json"): {
        "format": "json",
        "indent": 2,
        "encoding": "utf-8",
        "store_empty": False,
    },
    # For NDJSON instead, use this and comment the block above:
    # str(OUT_DIR / "announcements-%(time)s.ndjson"): {
    #     "format": "jsonlines",
    #     "encoding": "utf-8",
    #     "store_empty": False,
    # },
}

# --- Pipelines ---
# DO NOT set global ITEM_PIPELINES here because the bse_ann_api spider
# declares its own single pipeline (bse_scraper.pipelines_one.AnnouncementsPipeline)
# via custom_settings. Leaving this empty prevents loading your old pipelines.
# ITEM_PIPELINES = { }

# --- (optional) Default headers if you need them globally ---
# DEFAULT_REQUEST_HEADERS = {
#     "Accept": "application/json, text/plain, */*",
#     "Accept-Language": "en-US,en;q=0.9",
# }

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Playwright browser config
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000  # 30s

# IMPORTANT: use asyncio reactor (needed by scrapy-playwright)
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# ---- scrapy-playwright ----
# scrapy-playwright
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": ["--disable-blink-features=AutomationControlled"],
}
PLAYWRIGHT_CONTEXTS = {
    "stealth": {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "locale": "en-US",
        "timezone_id": "Asia/Kolkata",
        "java_script_enabled": True,
        "extra_http_headers": {
            "Accept-Language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Chromium";v="126", "Not=A?Brand";v="24", "Google Chrome";v="126"',
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-mobile": "?0",
        },
    }
}
