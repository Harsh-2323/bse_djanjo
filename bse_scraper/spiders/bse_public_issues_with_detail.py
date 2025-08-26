import re
from datetime import datetime
from urllib.parse import urljoin
import scrapy


def to_iso(dmy: str) -> str:
    """Convert dd-mm-YYYY (or dd/mm/YYYY) to YYYY-mm-dd; otherwise return trimmed."""
    s = (dmy or "").strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s


def split_price_band(s: str):
    """Return (min, max) from '105.00 - 111.00' etc."""
    s = (s or "").strip()
    if not s:
        return None, None
    parts = [p.strip() for p in re.split(r"[-–]", s) if p.strip()]
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], parts[-1]


CODE_MAP = {
    "DPI": "Debt Public Issue",
    "RI": "Rights Issue",
    "OTB": "Offer to Buy",
    "CMN": "Call Money Notice",
    "IPO": "IPO",
    "FPO": "FPO",
    "OFS": "Offer for Sale",
}

PDF_HINTS = ("newpdf.aspx", "/downloads/ipo/", ".pdf")


def looks_like_pdf(url: str) -> bool:
    if not url:
        return False
    u = url.strip().lower()
    return any(u.endswith(".pdf") or h in u for h in PDF_HINTS)


def extract_urls(base_url: str, href: str | None, onclick: str | None):
    """
    Collect and absolutize URLs from href and inline JS (window.open, location.href/assign).
    Handles absolute, '/relative', and bare 'relative' like 'BSEcumu_demand.aspx?...'
    """
    urls = set()

    def _add(u: str | None):
        if not u:
            return
        u = u.strip().strip("\"'")
        if (
            not u
            or u.lower().startswith("javascript:")
            or u.lower().startswith("mailto:")
            or u.lower().startswith("tel:")
        ):
            return
        urls.add(urljoin(base_url, u))

    # 1) plain href
    if href:
        _add(href)

    # 2) JS patterns (window.open / location.href / location.assign)
    js = " ".join(x for x in [onclick or "", href or ""] if x)

    # window.open('...')
    for m in re.finditer(r"window\.open\(\s*['\"]([^'\"]+)['\"]", js, flags=re.I):
        _add(m.group(1))

    # location.href='...' and location.assign('...')
    for m in re.finditer(r"location\.(?:href|assign)\s*=\s*['\"]([^'\"]+)['\"]", js, flags=re.I):
        _add(m.group(1))

    # generic quoted strings that look like paths (contain .aspx/.pdf or a slash)
    for m in re.finditer(
        r"['\"]([A-Za-z0-9_./?-][^'\"\s]*?(?:\.aspx|\.pdf)(?:\?[^'\"\s]*)?)['\"]",
        js,
        flags=re.I,
    ):
        _add(m.group(1))

    return urls


class BsePublicIssuesWithDetailSpider(scrapy.Spider):
    name = "bse_public_issues_with_detail"
    start_urls = [
        # Live public issues (main list)
        "https://www.bseindia.com/markets/PublicIssues/IPOIssues_new.aspx?id=1&Type=P"
    ]

    custom_settings = {
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "DOWNLOAD_DELAY": 1,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
    }

    # ---------- LIST PAGE ----------
    def parse(self, response):
        # Preferred table on the page (matches your sample HTML)
        rows = response.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblID']//tr[td]")

        # Fallback: any table with at least 8 TDs (defensive)
        if not rows:
            rows = response.xpath("//table//tr[count(td) >= 8]")

        self.logger.info("Candidate rows: %d", len(rows))

        for row in rows:
            # Skip headers
            header = row.xpath(".//th")
            if header:
                continue

            def cell(n):
                # Extract normalized text content of nth td
                return row.xpath(f"normalize-space(.//td[{n}]//text())").get(default="").strip()

            security_name = cell(1)
            if not security_name or security_name.lower() in {"security name", "scrip name"}:
                continue

            detail_href = row.xpath(".//td[1]//a/@href").get()
            exchange_platform = cell(2)
            start_date = cell(3)
            end_date = cell(4)
            offer_price = cell(5)
            face_value = cell(6)
            type_of_issue = cell(7)
            issue_status = cell(8)

            pmin, pmax = split_price_band(offer_price)

            base_item = {
                "security_name": security_name,
                "exchange_platform": exchange_platform,
                "start_date": to_iso(start_date),
                "end_date": to_iso(end_date),
                "offer_price": offer_price,
                "face_value": face_value,
                "type_of_issue": type_of_issue,
                "issue_status": issue_status,
                "price_min": pmin,
                "price_max": pmax,
                "type_of_issue_long": CODE_MAP.get((type_of_issue or "").upper(), type_of_issue),
                "list_url": response.url,
            }

            if detail_href and not detail_href.lower().startswith("javascript"):
                yield response.follow(
                    detail_href,
                    callback=self.parse_detail,
                    meta={"base_item": base_item},
                )
            else:
                yield base_item

    # ---------- DETAIL PAGE ----------
    def parse_detail(self, response):
        item = dict(response.meta.get("base_item") or {})
        item["detail_url"] = response.url

        # Focus on the UpdatePanel area first; fallback to whole document
        container = response.xpath("//div[@id='ctl00_ContentPlaceHolder1_UpdatePanel1']")
        if not container:
            container = response

        rows = container.xpath(".//tr")

        details = {}
        labeled_pdf_links = []
        labeled_other_links = []
        all_pdfs = set()

        prev_label = None
        prev_rowspan_left = 0

        for tr in rows:
            tds = tr.xpath("./td")
            if not tds:
                continue

            # Handle continuation rows when first cell had rowspan
            if len(tds) == 1 and prev_label and prev_rowspan_left > 0:
                cont = tds[0].xpath("normalize-space(.//text()[normalize-space()])").get("") or ""
                if cont:
                    details[prev_label] = (details.get(prev_label, "") + " " + cont).strip()
                prev_rowspan_left -= 1
                continue

            if len(tds) >= 2:
                label_raw = tds[0].xpath("normalize-space(.)").get("") or ""
                label = re.sub(r":\s*$", "", label_raw).strip()
                value_text = tds[1].xpath("normalize-space(.//text()[normalize-space()])").get("") or ""

                # Track rowspan (e.g., "Minimum Bid Quantity" spans 2 rows)
                prev_label = None
                prev_rowspan_left = 0
                rowspan = tds[0].xpath("@rowspan").get()
                try:
                    if rowspan:
                        prev_label = label
                        prev_rowspan_left = max(int(rowspan) - 1, 0)
                except Exception:
                    pass

                # Collect anchors and URLs from this cell (href + onclick JS)
                for a in tds[1].xpath(".//a"):
                    href = a.xpath("./@href").get()
                    onclick = a.xpath("./@onclick").get()
                    for url in extract_urls(response.url, href, onclick):
                        if looks_like_pdf(url):
                            all_pdfs.add(url)
                            labeled_pdf_links.append(
                                {"label": label or "link", "url": url}
                            )
                        else:
                            labeled_other_links.append(
                                {"label": label or "link", "url": url}
                            )

                # Store the textual detail if it's not just "Click Here"
                if label:
                    if value_text and value_text.lower() not in {"click here", "view", "download"}:
                        details[label] = value_text

        # Finalize item
        item["details"] = details
        item["pdf_links"] = sorted(labeled_pdf_links, key=lambda d: (d["label"], d["url"]))
        item["links"] = sorted(labeled_other_links, key=lambda d: (d["label"], d["url"]))
        item["documents"] = item["pdf_links"]  # optional alias if you prefer this key
        item["file_urls"] = sorted(all_pdfs)   # for FilesPipeline

        yield item
