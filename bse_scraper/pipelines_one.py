# bse_scraper/pipelines_one.py
import os, re, mimetypes, hashlib, logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bse_api.settings")
os.environ.setdefault("DJANGO_ALLOW_ASYNC_UNSAFE", "true")  # allow sync ORM in asyncio reactor
import django
django.setup()
# --- Django bootstrap inside Scrapy ---
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bse_api.settings")
import django
django.setup()

from django.db import transaction

from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
import boto3

from scrape.models import Announcement, AnnouncementAttachment

log = logging.getLogger(__name__)


# ---------- helpers ----------
def _slug(s: str, maxlen: int = 80) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9._\- ]+", " ", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s[:maxlen] or "file"

def _sha256_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _safe_ts(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("T", " "))
    except Exception:
        return None

def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


# ---------- the only pipeline you need ----------
class AnnouncementsPipeline(FilesPipeline):
    """
    Single pipeline that:
      1) downloads PDFs (FilesPipeline),
      2) uploads them to Cloudflare R2,
      3) saves Announcement + Attachments using Django ORM.
    """

    # ---- 1) download PDFs ----
    def get_media_requests(self, item, info):
        for url in item.get("file_urls") or []:
            yield Request(url, meta={"_ctx": item.get("_ctx", {}), "_headline": item.get("headline")})

    def file_path(self, request, response=None, info=None, *, item=None):
        ctx = request.meta.get("_ctx", {}) if request else {}
        dt = _safe_ts(ctx.get("dt")) or datetime.now()
        y, m, d = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        news_id = ctx.get("news_id") or ""
        company = _slug(ctx.get("company") or "")
        head = _slug(request.meta.get("_headline") or "")
        base = os.path.basename(request.url.split("?")[0])
        ext = os.path.splitext(base)[1] or ".pdf"
        return f"announcements/{y}/{m}/{d}/{news_id}_{company}_{head}{ext}"

    # ---- 2) R2 upload + 3) ORM save ----
    def item_completed(self, results, item, info):
        files_out: List[Dict[str, Any]] = []
        s3 = _r2_client()
        bucket = os.environ["R2_BUCKET"]
        public_base = (os.environ.get("R2_PUBLIC_BASEURL") or "").rstrip("/")

        # FS store dir
        basedir = getattr(self.store, "basedir", None)

        # For each downloaded file, upload to R2 and collect metadata
        for ok, data in results or []:
            if not ok:
                continue
            rel_path = data.get("path")
            orig_url = data.get("url")
            if not rel_path or not basedir:
                continue

            abs_path = os.path.abspath(os.path.join(basedir, rel_path))
            try:
                size = os.path.getsize(abs_path)
            except Exception:
                size = None

            mime, _ = mimetypes.guess_type(abs_path)
            if not mime:
                mime = "application/pdf"

            sha = _sha256_file(abs_path) or ""
            r2_key = f"bse/{rel_path}"

            with open(abs_path, "rb") as f:
                s3.put_object(Bucket=bucket, Key=r2_key, Body=f, ContentType=mime)

            r2_url = f"{public_base}/{r2_key}" if public_base else None

            files_out.append({
                "url": orig_url,
                "path": rel_path,
                "abs_path": abs_path,
                "size_bytes": size,
                "sha256": sha,
                "mime": mime,
                "r2_key": r2_key,
                "r2_url": r2_url,
                "bucket": bucket,
            })

        # attach files_out back on the item (useful for FEEDS export / debugging)
        item["files"] = files_out

        # ---- ORM save (Announcement + Attachments) ----
        # Map core fields
        ann_data = {
            "news_id": item.get("news_id"),
            "scrip_cd": item.get("scrip_cd"),
            "company_name": item.get("company_name"),
            "segment": item.get("segment"),
            "category": item.get("category"),
            "subcategory": item.get("subcategory"),
            "headline": item.get("headline"),
            "body_html": item.get("body_html"),
            "body_text": item.get("body_text"),
            "dissem_dt_ist": item.get("dissem_dt_ist") or None,
            "dissem_dt_utc": item.get("dissem_dt_utc") or None,
            "received_dt_ist": item.get("received_dt_ist") or None,
            "time_taken_sec": item.get("time_taken_sec"),
            "pdf_url": item.get("pdf_url"),
            "pdf_size_bytes": item.get("pdf_size_bytes"),
            "xbrl_url": item.get("xbrl_url"),
            "av_url": item.get("av_url"),
            "has_pdf": bool(item.get("has_pdf")),
            "has_xbrl": bool(item.get("has_xbrl")),
            "has_av": bool(item.get("has_av")),
            "is_revision": bool(item.get("is_revision")),
            "reg_tags": item.get("reg_tags") or [],
            "company_url": item.get("company_url"),
            "source_url": item.get("source_url"),
            "page_no": item.get("page_no"),
            "payload": dict(item),   # keep the raw/enriched item too
        }

        news_id = ann_data["news_id"] or ""
        if not news_id:
            # Fallback: derive a stable id (rare, but safe-guard)
            import hashlib
            base = "|".join([
                str(ann_data["pdf_url"] or ""),
                str(ann_data["headline"] or ""),
                str(ann_data["dissem_dt_ist"] or ""),
                str(ann_data["scrip_cd"] or "")
            ])
            news_id = hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]
            ann_data["news_id"] = news_id

        with transaction.atomic():
            ann_obj, _ = Announcement.objects.update_or_create(
                news_id=news_id,
                defaults=ann_data,
            )

            # Merge declared attachments with files_out (R2 info)
            # Build quick map for uploaded files by original url
            by_url = {f["url"]: f for f in files_out}

            declared = item.get("attachments_declared") or []
            to_upsert = []

            for att in declared:
                url = att.get("url")
                if not url:
                    continue
                f = by_url.get(url)
                to_upsert.append({
                    "url": url,
                    "kind": att.get("kind") or "pdf",
                    "size_bytes": (f.get("size_bytes") if f else att.get("size_bytes")),
                    "mime": f.get("mime") if f else None,
                    "local_path": f.get("abs_path") if f else None,
                    "sha256": f.get("sha256") if f else None,
                    "r2_key": f.get("r2_key") if f else None,
                    "r2_url": f.get("r2_url") if f else None,
                    "bucket": f.get("bucket") if f else None,
                })

            # Upsert each attachment
            for row in to_upsert:
                obj, _ = AnnouncementAttachment.objects.update_or_create(
                    announcement=ann_obj,
                    url=row["url"],
                    defaults=row,
                )

        log.info("Saved announcement %s with %d attachment(s)", news_id, len(to_upsert))
        return item
