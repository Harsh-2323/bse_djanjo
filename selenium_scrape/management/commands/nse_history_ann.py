# selenium_scrape/management/commands/nse_history_ann.py
from django.core.management.base import BaseCommand
import requests, json, time, re
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin
from pathlib import Path

BASE_URL = "https://www.nseindia.com/api/corporate-announcements"
NSE_HOST = "https://www.nseindia.com"

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    # Avoid brotli unless you installed it
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-corp-announcements",
    "X-Requested-With": "XMLHttpRequest",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

BLOCK_PATTERNS = (
    "Access Denied",
    "Request unsuccessful",
    "Akamai",
    "Attention Required",
    "<html", "<!doctype html",
)

def make_session():
    s = requests.Session()
    s.headers.update(COMMON_HEADERS)
    retry = Retry(
        total=5, read=5, connect=5,
        backoff_factor=1.0,
        status_forcelist=(401, 403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def warm_up(session, symbol_for_warmup="RELIANCE", sleep_between=1.0, debug=False):
    urls = [
        "https://www.nseindia.com/",
        "https://www.nseindia.com/companies-listing/corporate-filings-corp-announcements",
        f"https://www.nseindia.com/get-quotes/equity?symbol={symbol_for_warmup}",
    ]
    for u in urls:
        try:
            r = session.get(u, timeout=20, allow_redirects=True)
            if debug:
                ct = r.headers.get("Content-Type", "")
                print(f"[warm] {u} → {r.status_code} ({ct})")
        except requests.RequestException as e:
            if debug:
                print(f"[warm] {u} errored: {e}")
        time.sleep(sleep_between)

_XSSI_PREFIXES = (")]}',", "while(1);", "for(;;);")

def _strip_xssi(text: str) -> str:
    for p in _XSSI_PREFIXES:
        if text.startswith(p):
            text = text[len(p):].lstrip()
            break
    m = re.search(r'[\{\[]', text)
    if m and m.start() > 0:
        text = text[m.start():]
    return text

def parse_json_or_block(resp, debug=False):
    text = resp.text or ""
    preview = text[:200].replace("\n", " ")
    if debug:
        print(f"[parse] {resp.status_code} {resp.headers.get('Content-Type','')} len={len(text)} preview={preview!r}")

    if resp.status_code != 200:
        raise ValueError(f"HTTP {resp.status_code}: {preview}")
    if not text.strip():
        raise ValueError("Empty response body")

    low = text.lower().strip()
    if any(p in low for p in (b.lower() for b in BLOCK_PATTERNS)):
        raise ValueError(f"Blocked/HTML page returned: {preview}")

    text = _strip_xssi(text.strip())

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        m = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
        if m:
            return json.loads(m.group(1))
        raise ValueError(f"Invalid JSON: {e}: {preview}")

def daterange(start_dt, end_dt, step_days=30):
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def fetch_announcements(session, symbol, from_date, to_date, issuer=None, max_attempts=4, debug=False):
    params = {
        "index": "equities",
        "from_date": from_date,
        "to_date": to_date,
        "symbol": symbol,
        "reqXbrl": "false",
    }
    if issuer:
        params["issuer"] = issuer

    last_err = None
    for attempt in range(1, max_attempts + 1):
        r = session.get(BASE_URL, params=params, timeout=30)
        try:
            return parse_json_or_block(r, debug=debug)
        except ValueError as e:
            last_err = e
            warm_up(session, symbol_for_warmup=symbol, sleep_between=1.1 + 0.3*attempt, debug=debug)
            time.sleep(0.8 + attempt * 0.6)
            continue
    raise RuntimeError(f"NSE blocked/unavailable for {symbol} {from_date}→{to_date}: {last_err}")

# ------------------------
# Normalization helpers
# ------------------------

def _safe_get(d: dict, *keys, default=""):
    """Return the first existing key's value from d, else default."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def _first_attachment_url(row: dict) -> str:
    """
    attchmntFile can be:
      - a string URL
      - a list of dicts: [{"url": "...", "name": "..."}]
      - a list of strings
      - under 'attachment' / 'attachments'
    Return absolute URL or "".
    """
    v = row.get("attchmntFile")
    if isinstance(v, str) and v:
        return urljoin(NSE_HOST, v)

    if isinstance(v, list) and v:
        first = v[0]
        if isinstance(first, dict) and "url" in first and first["url"]:
            return urljoin(NSE_HOST, first["url"])
        if isinstance(first, str) and first:
            return urljoin(NSE_HOST, first)

    # fallback: other keys NSE sometimes uses
    alt = row.get("attachment") or row.get("attachments")
    if isinstance(alt, list) and alt:
        first = alt[0]
        if isinstance(first, dict) and "url" in first and first["url"]:
            return urljoin(NSE_HOST, first["url"])
        if isinstance(first, str) and first:
            return urljoin(NSE_HOST, first)

    return ""

def _to_iso_sort_date(an_dt_text: str) -> str:
    """
    Convert '05-Sep-2025 10:33:20' -> '2025-09-05 10:33:20'.
    If parse fails, return "".
    """
    if not an_dt_text:
        return ""
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            return datetime.strptime(an_dt_text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return ""

def _compute_difference(a: str, b: str) -> str:
    """
    Return HH:MM:SS difference between two date-time strings like '05-Sep-2025 10:33:20'.
    If anything fails, return "".
    """
    def parse(s: str):
        for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    dt1, dt2 = parse(a), parse(b)
    if not dt1 or not dt2:
        return ""
    delta = abs(dt2 - dt1)
    total = int(delta.total_seconds())
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def to_target_schema(row: dict) -> dict:
    """
    Produce the exact 12-field object you requested:
    symbol, desc, attchmntFile, sm_name, sm_isin, an_dt, sort_date,
    attchmntText, exchdisstime, difference, fileSize, attFileSize
    """
    # date/times
    an_dt = _safe_get(row, "an_dt", "announceTime", default="")
    exchdisstime = _safe_get(row, "exchdisstime", "exchangeDisseminatedTime", default="")
    sort_date = _safe_get(row, "sort_date", default="") or _to_iso_sort_date(an_dt)

    # difference (prefer server value; else compute)
    difference = _safe_get(row, "difference", default="")
    if not difference and an_dt and exchdisstime:
        difference = _compute_difference(an_dt, exchdisstime)

    # attachment URL
    att_url = _first_attachment_url(row)

    # sizes
    file_size = _safe_get(row, "fileSize", default="")
    att_file_size = _safe_get(row, "attFileSize", default="") or file_size

    # desc/headline + attachment text
    desc = _safe_get(row, "desc", "headline", "subject", default="")
    att_text = _safe_get(row, "attchmntText", "moreText", "sm_desc", default="")

    return {
        "symbol": _safe_get(row, "symbol", default=""),
        "desc": desc,
        "attchmntFile": att_url,
        "sm_name": _safe_get(row, "sm_name", "companyName", default=""),
        "sm_isin": _safe_get(row, "sm_isin", "isin", default=""),
        "an_dt": an_dt,
        "sort_date": sort_date,
        "attchmntText": att_text,
        "exchdisstime": exchdisstime,
        "difference": difference,
        "fileSize": file_size,
        "attFileSize": att_file_size,
    }

# ------------------------
# Django command
# ------------------------

class Command(BaseCommand):
    help = "Fetch NSE historical corporate announcements and save/print results (normalized to 12 fields)."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Company symbol (e.g., ABB, RELIANCE)")
        parser.add_argument("from_date", type=str, help="Start date dd-mm-yyyy")
        parser.add_argument("to_date", type=str, help="End date dd-mm-yyyy")
        parser.add_argument("--issuer", type=str, default=None, help="Company full name (optional)")
        parser.add_argument("--chunk", type=int, default=30, help="Days per chunk (default: 30)")
        parser.add_argument("--sleep", type=float, default=0.8, help="Sleep between API calls (sec)")
        parser.add_argument("--debug", action="store_true", help="Print debug info for responses")

        # Output controls
        parser.add_argument("--out", type=str, default=None, help="Output JSON file path")
        parser.add_argument("--ndjson", action="store_true", help="Write line-delimited JSON (one row per line)")
        parser.add_argument("--no-print", dest="no_print", action="store_true", help="Do not print the large JSON to terminal")

        # NEW: pretty-printed array alongside NDJSON or when explicitly requested
        parser.add_argument(
            "--pretty-file",
            dest="pretty_file",
            type=str,
            default=None,
            help="Also write a pretty-printed JSON array to this path. "
                 "If omitted and --ndjson is used, a sibling '<out>_pretty.json' will be created."
        )

    def handle(self, *args, **opts):
        symbol = opts["symbol"].upper().strip()
        issuer = opts["issuer"]
        start = datetime.strptime(opts["from_date"], "%d-%m-%Y")
        end = datetime.strptime(opts["to_date"], "%d-%m-%Y")
        chunk_days = int(opts["chunk"])
        pause = float(opts["sleep"])
        debug = bool(opts["debug"])

        # decide output path
        if opts["out"]:
            out_path = Path(opts["out"])
            out_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            out_dir = Path("outputs") / "nse_announcements"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_fname = f"{symbol}_{start.strftime('%d-%m-%Y')}_{end.strftime('%d-%m-%Y')}.json"
            out_path = out_dir / out_fname

        # decide pretty path
        pretty_path = None
        if opts.get("pretty_file"):
            pretty_path = Path(opts["pretty_file"])
            pretty_path.parent.mkdir(parents=True, exist_ok=True)
        elif opts["ndjson"]:
            # auto place pretty file next to main output if using NDJSON
            pretty_path = out_path.with_name(out_path.stem + "_pretty.json")

        self.stdout.write(self.style.NOTICE(
            f"Fetching announcements for {symbol} {start.date()} → {end.date()}"
        ))

        session = make_session()
        warm_up(session, symbol_for_warmup=symbol, debug=debug)

        all_rows = []
        for s, e in daterange(start, end, chunk_days):
            fd = s.strftime("%d-%m-%Y")
            td = e.strftime("%d-%m-%Y")
            try:
                data = fetch_announcements(session, symbol, fd, td, issuer=issuer, debug=debug)
                rows = data.get("rows", []) if isinstance(data, dict) else data
                self.stdout.write(f"🔹 {len(rows)} announcements for {fd} → {td}")

                # Normalize each row to the 12-field schema
                normalized = [to_target_schema(r) for r in rows]
                all_rows.extend(normalized)

                time.sleep(pause)
            except Exception as ex:
                self.stderr.write(f"⚠️  {fd} → {td}: {ex}")
                # continue

        # --- write main file ---
        if opts["ndjson"]:
            with out_path.open("w", encoding="utf-8") as f:
                for row in all_rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        else:
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(all_rows, f, indent=2, ensure_ascii=False)

        self.stdout.write(self.style.SUCCESS(f"Saved file: {out_path.resolve()}"))
        self.stdout.write(self.style.SUCCESS(f"Total rows: {len(all_rows)}"))

        # --- optionally write pretty JSON array (no data changes, just formatting) ---
        if pretty_path:
            if pretty_path.resolve() != out_path.resolve():
                with pretty_path.open("w", encoding="utf-8") as f:
                    json.dump(all_rows, f, indent=2, ensure_ascii=False)
                self.stdout.write(self.style.SUCCESS(f"Wrote pretty file: {pretty_path.resolve()}"))

        if not opts.get("no_print"):
            self.stdout.write(json.dumps(all_rows, indent=2, ensure_ascii=False))
