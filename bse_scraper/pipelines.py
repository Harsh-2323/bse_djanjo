import os
import psycopg2
from psycopg2.extras import Json
import boto3
import requests
from urllib.parse import urlparse
from datetime import datetime, date
from scrapy.exceptions import DropItem

def _to_date(v):
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if not v:
        return None
    try:
        # spider emits YYYY-mm-dd strings -> convert to date
        return datetime.strptime(str(v), "%Y-%m-%d").date()
    except Exception:
        return None

def _to_float(v):
    if v is None:
        return None
    try:
        # handles '533.00', '533', '533,00'
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None

class PostgresPipeline:
    """Save IPO items into Postgres public_issues table."""

    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            dbname=os.getenv("PGDATABASE", "bse_scraper"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "root123"),
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
        )
        self.cur = self.conn.cursor()
        spider.logger.info("✅ Connected to Postgres")

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        spider.logger.info("🔒 Closed Postgres connection")

    def process_item(self, item, spider):
        if not item.get("security_name"):
            raise DropItem("❌ Missing security_name")

        sql = """
            INSERT INTO public_issues
            (security_name, exchange_platform, start_date, end_date, offer_price,
             face_value, type_of_issue, issue_status, price_min, price_max,
             type_of_issue_long, list_url, detail_url,
             details, pdf_links, links, documents, file_urls, uploaded_files,
             created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT DO NOTHING;
        """
        values = (
            item.get("security_name"),
            item.get("exchange_platform"),
            _to_date(item.get("start_date")),
            _to_date(item.get("end_date")),
            item.get("offer_price"),
            item.get("face_value"),
            item.get("type_of_issue"),
            item.get("issue_status"),
            _to_float(item.get("price_min")),
            _to_float(item.get("price_max")),
            item.get("type_of_issue_long"),
            item.get("list_url"),
            item.get("detail_url"),
            Json(item.get("details") or {}),        # ✅ psycopg2 Json wrapper
            Json(item.get("pdf_links") or []),
            Json(item.get("links") or []),
            Json(item.get("documents") or []),
            Json(item.get("file_urls") or []),
            Json(item.get("uploaded_files") or []),
        )

        try:
            spider.logger.debug(f"📝 SQL values for {item.get('security_name')}: {values}")
            self.cur.execute(sql, values)
            self.conn.commit()
            spider.logger.info(f"✅ Inserted issue: {item.get('security_name')}")
        except Exception as e:
            # surface exact DB error (and SQLSTATE) in logs
            err = getattr(e, "diag", None)
            code = getattr(err, "sqlstate", None)
            message = getattr(err, "message_primary", None)
            spider.logger.error(f"❌ Postgres insert failed [{code}]: {e} | primary={message}")
            self.conn.rollback()
            # re-raise so it appears in your API log_tail if desired
            # raise
        return item


class R2Pipeline:
    """Upload PDFs/ZIPs to Cloudflare R2 and store public URLs."""

    def open_spider(self, spider):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("R2_ENDPOINT"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        )
        self.bucket = os.getenv("R2_BUCKET", "market-filings")
        self.public_baseurl = os.getenv("R2_PUBLIC_BASEURL", "")
        spider.logger.info("✅ Connected to R2")

    def process_item(self, item, spider):
        uploaded_files = []

        for url in item.get("file_urls", []):
            try:
                spider.logger.debug(f"⬇️ Downloading {url}")
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    filename = os.path.basename(urlparse(url).path)
                    # R2/S3 allow spaces in keys but URLs look cleaner URL-encoded:
                    key = f"bse_public_issues/{filename}"
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=resp.content)
                    public_url = f"{self.public_baseurl}/{key}" if self.public_baseurl else None
                    uploaded_files.append({"source": url, "r2_key": key, "public_url": public_url})
                    spider.logger.info(f"📂 Uploaded {filename} → {public_url or key}")
                else:
                    spider.logger.warning(f"⚠️ Fetch failed {url}: HTTP {resp.status_code}")
            except Exception as e:
                spider.logger.error(f"❌ Upload failed for {url}: {e}")

        item["uploaded_files"] = uploaded_files
        return item
