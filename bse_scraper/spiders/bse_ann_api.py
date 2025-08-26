# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import scrapy

# --- Timezones (IST only output; use UTC internally if needed) ---
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

# --- Helpers ---

def _safe_json(text: str):
    try:
        raw = json.loads(text) if text else {}
    except Exception:
        return {"Table": [], "Table1": []}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "{}")
        except Exception:
            return {"Table": [], "Table1": []}
    if isinstance(raw, list):
        return {"Table": raw, "Table1": []}
    if isinstance(raw, dict):
        t = raw.get("Table") or raw.get("data") or raw.get("Data") or []
        if isinstance(t, str):
            try:
                t = json.loads(t)
            except Exception:
                t = []
        return {"Table": t or [], "Table1": raw.get("Table1") or []}
    return {"Table": [], "Table1": []}


def _strip_html(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip() or None


def _parse_bse_dt(val: str | None):
    """
    Parse a BSE timestamp and return a timezone-aware IST datetime.

    Accepts:
      - '25-08-2025 17:08:08'
      - '2025-08-25 17:08:08'
      - '25 Aug 2025 17:08:08'
      - '25/08/2025 17:08:08'
      - ISO-like strings (e.g., '2025-08-25T17:08:08Z', '2025-08-25T17:08:08+05:30')
      - '/Date(1692949028000)/'  (ms epoch)
    Returns:
      datetime (aware, IST) or None
    """
    if not val:
        return None
    s = str(val).strip()

    # /Date(…)/ in ms
    m = re.match(r"/Date\((\d+)\)/", s)
    if m:
        ts = int(m.group(1)) / 1000.0
        return datetime.fromtimestamp(ts, tz=UTC).astimezone(IST)

    # ISO-like
    try:
        iso_s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST)
    except Exception:
        pass

    # Common explicit patterns
    patterns = [
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d %b %Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    for fmt in patterns:
        try:
            dt = datetime.strptime(s, fmt)
            # Assume given string is in IST if naive
            return dt.replace(tzinfo=IST)
        except Exception:
            continue

    return None


def _fmt_ist(dt: datetime | None) -> str | None:
    """Return 'YYYY-MM-DD HH:MM:SS' in IST (no offset text)."""
    if not dt:
        return None
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def _seconds_from_hhmmss(s: str | None) -> int | None:
    if not s:
        return None
    m = re.match(r"^\s*(\d{2}):(\d{2}):(\d{2})\s*$", str(s))
    if not m:
        return None
    h, mi, se = map(int, m.groups())
    return h * 3600 + mi * 60 + se


def _is_revision(text: str) -> bool:
    if not text:
        return False
    return re.search(
        r"\b(revised?|corrigendum|addendum|clarification|resubmission|read\s+with)\b",
        text,
        flags=re.I,
    ) is not None


def _reg_tags(text: str) -> list[str]:
    if not text:
        return []
    tags = []
    for rx, label in [
        (r"\breg(?:ulation)?\s*30\b", "Reg 30"),
        (r"\breg(?:ulation)?\s*33\b", "Reg 33"),
        (r"\breg(?:ulation)?\s*42\b", "Reg 42"),
        (r"\bLODR\b", "LODR"),
        (r"\bSAST\b", "SAST"),
        (r"\bPIT\b", "PIT"),
    ]:
        if re.search(rx, text, flags=re.I):
            tags.append(label)
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# --- Spider ---

class BSEAnnAPI(scrapy.Spider):
    name = "bse_ann_api"
    allowed_domains = ["api.bseindia.com", "www.bseindia.com"]

    # SINGLE pipeline
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.25,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.bseindia.com/corporates/ann.html",
            "Origin": "https://www.bseindia.com",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
        },
        "ITEM_PIPELINES": {
            "bse_scraper.pipelines_one.AnnouncementsPipeline": 200,
        },
    }

    BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

    def __init__(
        self,
        pages: int = 1,
        segment: str = "C",
        strCat: str = "-1",
        subcategory: str = "-1",
        scrip: str = "",
        from_date: str | None = None,
        to_date: str | None = None,
        strSearch: str = "P",
        **kw,
    ):
        super().__init__(**kw)
        self.pages = max(1, int(pages)) if str(pages).isdigit() else 1
        self.segment = segment or "C"
        self.strCat = strCat or "-1"
        self.subcategory = subcategory or "-1"
        self.scrip = scrip or ""
        self.strSearch = strSearch or "P"

        def norm_date(v: str | None):
            if not v:
                return datetime.now(tz=IST).strftime("%Y%m%d")
            v = v.strip()
            if len(v) == 8 and v.isdigit():
                return v
            try:
                return datetime.strptime(v, "%d/%m/%Y").strftime("%Y%m%d")
            except Exception:
                return datetime.now(tz=IST).strftime("%Y%m%d")

        self.prev_date = norm_date(from_date)
        self.to_date = norm_date(to_date)

    def _build_url(self, pageno: int) -> str:
        q = {
            "pageno": pageno,
            "strCat": self.strCat,
            "strPrevDate": self.prev_date,
            "strScrip": self.scrip,
            "strSearch": self.strSearch,
            "strToDate": self.to_date,
            "strType": self.segment,
            "subcategory": self.subcategory,
        }
        return f"{self.BASE_URL}?{urlencode(q)}"

    def start_requests(self):
        for p in range(1, self.pages + 1):
            yield scrapy.Request(
                self._build_url(p),
                callback=self.parse,
                cb_kwargs={"pageno": p},
                dont_filter=True,
            )

    def parse(self, response, pageno: int):
        payload = _safe_json(response.text)
        for r in payload.get("Table") or []:
            if not isinstance(r, dict):
                continue

            news_id = r.get("NEWSID") or r.get("NEWS_ID") or r.get("NID")
            scrip_cd = r.get("SCRIP_CD") or r.get("SC_CODE") or r.get("SCRIPCODE")
            company = (
                r.get("SLONGNAME")
                or r.get("COMPANYNAME")
                or r.get("SCRIP_NAME")
                or r.get("SC_NAME")
            )
            company_name = (
                f"{company} - {scrip_cd}" if company and scrip_cd else (company or scrip_cd)
            )

            category = r.get("CATEGORYNAME") or None
            subcat = r.get("SUBCAT") or None
            subject = r.get("NEWSSUB") or None
            headline = r.get("HEADLINE") or subject
            more_html = r.get("MORE") or None
            body_text = _strip_html(more_html) or _strip_html(headline)

            # Timestamps -> IST only
            dissem_dt = _parse_bse_dt(
                r.get("Exchange Disseminated Time")
                or r.get("DT_TM")
                or r.get("NEWS_DT")
            )
            received_dt = _parse_bse_dt(
                r.get("Exchange Received Time") or r.get("NEWS_DT")
            )
            time_taken_sec = _seconds_from_hhmmss(r.get("TimeDiff"))

            # Attachments
            pdf_url, pdf_size_bytes = None, None
            attach = r.get("ATTACHMENTNAME")
            pdfflag = r.get("PDFFLAG")
            try:
                pdfflag = int(pdfflag) if pdfflag is not None else None
            except Exception:
                pdfflag = None

            if isinstance(attach, str) and attach.startswith(("http://", "https://")):
                pdf_url = attach
            elif attach:
                if pdfflag == 0:
                    pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attach}"
                elif pdfflag == 1:
                    pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{attach}"
                elif pdfflag == 2:
                    x = r.get("XML_NAME")
                    if isinstance(x, str) and x.startswith(("http://", "https://")):
                        pdf_url = x

            if r.get("Fld_Attachsize") is not None:
                try:
                    pdf_size_bytes = int(str(r.get("Fld_Attachsize")).strip())
                except Exception:
                    pdf_size_bytes = None

            file_status = (r.get("FILESTATUS") or "").strip().upper()
            xbrl_url = r.get("XML_NAME") if file_status == "X" else None
            av_url = r.get("AUDIO_VIDEO_FILE") or None

            hay = f"{subject or ''} {headline or ''} {more_html or ''}"
            is_revision = _is_revision(hay)
            tags = _reg_tags(hay)

            company_url = r.get("NSURL") or None
            source_url = self._build_url(pageno)
            file_urls = [pdf_url] if pdf_url else []

            yield {
                "news_id": str(news_id) if news_id is not None else None,
                "scrip_cd": str(scrip_cd) if scrip_cd is not None else None,
                "company_name": company_name,
                "segment": self.segment,
                "category": category,
                "subcategory": subcat,
                "headline": headline,
                "body_html": more_html,
                "body_text": body_text,
                # IST-only strings
                "dissem_dt_ist": _fmt_ist(dissem_dt),
                "received_dt_ist": _fmt_ist(received_dt),
                "time_taken_sec": time_taken_sec,
                "has_pdf": bool(pdf_url),
                "pdf_url": pdf_url,
                "pdf_size_bytes": pdf_size_bytes,
                "has_xbrl": bool(xbrl_url),
                "xbrl_url": xbrl_url,
                "has_av": bool(av_url),
                "av_url": av_url,
                "is_revision": is_revision,
                "reg_tags": tags,
                "company_url": company_url,
                "source_url": source_url,
                "page_no": pageno,
                "attachments_declared": [
                    *(
                        [{"url": pdf_url, "kind": "pdf", "size_bytes": pdf_size_bytes}]
                        if pdf_url
                        else []
                    ),
                    *(
                        [{"url": xbrl_url, "kind": "xbrl", "size_bytes": None}]
                        if xbrl_url
                        else []
                    ),
                    *(
                        [{"url": av_url, "kind": "audio_video", "size_bytes": None}]
                        if av_url
                        else []
                    ),
                ],
                "file_urls": file_urls,
                "_ctx": {
                    "company": company_name or "",
                    "scrip": str(scrip_cd) if scrip_cd else "",
                    "news_id": str(news_id) if news_id else "",
                    "dissem_dt_ist": _fmt_ist(dissem_dt),
                },
            }
