from django.db import models


class SeleniumAnnouncement(models.Model):
    id = models.BigAutoField(primary_key=True)

    company_name = models.CharField(max_length=255, null=True, blank=True)
    company_code = models.CharField(max_length=20, null=True, blank=True)

    # NEW
    headline = models.TextField(null=True, blank=True)
    category = models.CharField(max_length=120, null=True, blank=True, db_index=True)

    # Legacy (kept for compatibility with existing code)
    announcement_text = models.TextField(null=True, blank=True)

    exchange_received_date = models.CharField(max_length=20, null=True, blank=True)
    exchange_received_time = models.CharField(max_length=20, null=True, blank=True)
    exchange_disseminated_date = models.CharField(max_length=20, null=True, blank=True)
    exchange_disseminated_time = models.CharField(max_length=20, null=True, blank=True)

    pdf_link_web = models.TextField(null=True, blank=True)
    pdf_path_local = models.TextField(null=True, blank=True)
    pdf_path_cloud = models.TextField(null=True, blank=True)  # Stores R2 public URL
    pdf_r2_path = models.TextField(null=True, blank=True)    # Stores R2 object key (optional)

    # New fields
    attachment_size = models.CharField(max_length=50, null=True, blank=True)
    xbrl_nse_symbol = models.CharField(max_length=40, null=True, blank=True)
    xbrl_company_name = models.CharField(max_length=255, null=True, blank=True)
    xbrl_subject = models.TextField(null=True, blank=True)
    xbrl_description = models.TextField(null=True, blank=True)
    xbrl_attachment_url = models.TextField(null=True, blank=True)
    xbrl_datetime = models.CharField(max_length=40, null=True, blank=True)
    xbrl_category = models.CharField(max_length=120, null=True, blank=True)
    xbrl_parse_status = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "selenium_announcements"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["company_code", "exchange_disseminated_date", "exchange_disseminated_time"]),
            models.Index(fields=["category"]),
        ]

    def __str__(self):
        txt = self.headline or self.announcement_text or ""
        return f"{self.company_name or ''} - {txt[:50]}"

from django.db import models

from django.db import models

class NseAnnouncement(models.Model):
    id = models.BigAutoField(primary_key=True)

    # Core identifiers
    symbol = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)

    # Text / subject / details
    subject = models.TextField(null=True, blank=True)
    details = models.TextField(null=True, blank=True, help_text="Detailed announcement content")

    # Timestamps - separate date and time fields
    exchange_received_date = models.CharField(max_length=20, null=True, blank=True)
    exchange_received_time_only = models.CharField(max_length=20, null=True, blank=True)
    exchange_disseminated_date = models.CharField(max_length=20, null=True, blank=True)
    exchange_disseminated_time_only = models.CharField(max_length=20, null=True, blank=True)

    # Attachments
    attachment_size = models.CharField(max_length=50, null=True, blank=True)
    attachment_link = models.TextField(null=True, blank=True)   # may contain multiple PDFs
    xbrl_link = models.TextField(null=True, blank=True)         # may contain multiple XBRLs
    has_xbrl = models.BooleanField(default=False)

    # PDF storage fields (added to match SeleniumAnnouncement)
    pdf_link_web = models.TextField(null=True, blank=True, help_text="Web URL for the PDF")
    pdf_path_local = models.TextField(null=True, blank=True, help_text="Local file path for the PDF")
    pdf_path_cloud = models.TextField(null=True, blank=True, help_text="R2 public URL for the PDF")
    pdf_r2_path = models.TextField(null=True, blank=True, help_text="R2 object key for the PDF")

    # XBRL parsed fields
    xbrl_nse_symbol = models.CharField(max_length=40, null=True, blank=True)
    xbrl_company_name = models.CharField(max_length=255, null=True, blank=True)
    xbrl_subject = models.TextField(null=True, blank=True)
    xbrl_description = models.TextField(null=True, blank=True)
    xbrl_attachment_url = models.TextField(null=True, blank=True)
    xbrl_datetime = models.CharField(max_length=40, null=True, blank=True)
    xbrl_category = models.CharField(max_length=120, null=True, blank=True)
    xbrl_parse_status = models.CharField(max_length=50, null=True, blank=True)

    # Meta
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "nse_announcements"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["symbol", "exchange_disseminated_time_only"]),
            models.Index(fields=["xbrl_category"]),
            models.Index(fields=["details"], name="idx_nse_ann_details"),
            models.Index(fields=["exchange_received_date"], name="idx_nse_ann_recv_date"),
            models.Index(fields=["exchange_disseminated_date"], name="idx_nse_ann_diss_date"),
            models.Index(fields=["symbol", "exchange_disseminated_date"], name="idx_nse_ann_symbol_diss_date"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["symbol", "subject", "exchange_disseminated_time_only"],
                name="uniq_nse_ann_symbol_subject_time",
            )
        ]

    def __str__(self):
        txt = self.subject or self.xbrl_subject or ""
        return f"{self.symbol or self.company_name or ''} - {txt[:60]}"

    def get_details_preview(self, max_length=200):
        """Return a truncated version of details for display purposes."""
        if not self.details:
            return ""
        if len(self.details) <= max_length:
            return self.details
        return self.details[:max_length].rsplit(' ', 1)[0] + "..."

    def get_exchange_received_datetime(self):
        """Combine separate date and time fields into a single datetime string."""
        if self.exchange_received_date and self.exchange_received_time_only:
            return f"{self.exchange_received_date} {self.exchange_received_time_only}"
        return ""

    def get_exchange_disseminated_datetime(self):
        """Combine separate date and time fields into a single datetime string."""
        if self.exchange_disseminated_date and self.exchange_disseminated_time_only:
            return f"{self.exchange_disseminated_date} {self.exchange_disseminated_time_only}"
        return ""

class CorporateAction(models.Model):
    id = models.BigAutoField(primary_key=True)
    
    # Company details
    company_name = models.CharField(max_length=255, null=True, blank=True)
    bse_code = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    security_name = models.CharField(max_length=100, null=True, blank=True)
    
    # Corporate actions data (JSON field)
    actions_data = models.JSONField(null=True, blank=True, help_text="JSON array of all corporate actions for this company")
    
    # File storage details
    csv_r2_path = models.TextField(null=True, blank=True, help_text="R2 object key path")
    csv_cloud_url = models.TextField(null=True, blank=True, help_text="R2 public URL for CSV file")
    
    # Metadata
    total_actions_count = models.IntegerField(default=0, help_text="Number of corporate actions in this record")
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "corporate_actions"
        ordering = ["-last_updated"]
        indexes = [
            models.Index(fields=["bse_code"]),
            models.Index(fields=["company_name"]),
            models.Index(fields=["last_updated"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["bse_code"],
                name="uniq_corp_action_bse_code",
            )
        ]
    
    def __str__(self):
        return f"{self.company_name or 'Unknown'} ({self.bse_code or 'No BSE Code'}) - {self.total_actions_count} actions"
    
    def save(self, *args, **kwargs):
        # Auto-calculate total_actions_count when saving
        if self.actions_data and isinstance(self.actions_data, list):
            self.total_actions_count = len(self.actions_data)
        super().save(*args, **kwargs)


# NEW MODEL: Consolidated BSE Announcements (similar to CorporateAction)
class BseAnnouncementAggregate(models.Model):
    id = models.BigAutoField(primary_key=True)
    
    # Company details
    company_name = models.CharField(max_length=255, null=True, blank=True)
    bse_code = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    
    # Consolidated announcements data (JSON field)
    announcements_data = models.JSONField(null=True, blank=True, help_text="JSON array of all announcements for this company")
    
    # Date range for this scrape
    scrape_start_date = models.CharField(max_length=20, null=True, blank=True, help_text="Start date in DD-MM-YYYY format")
    scrape_end_date = models.CharField(max_length=20, null=True, blank=True, help_text="End date in DD-MM-YYYY format")
    
    # PDF storage tracking (JSON field containing list of PDF info)
    pdfs_data = models.JSONField(null=True, blank=True, help_text="JSON array of PDF storage information")
    
    # Metadata
    total_announcements_count = models.IntegerField(default=0, help_text="Number of announcements in this record")
    total_pdfs_count = models.IntegerField(default=0, help_text="Number of PDFs stored in cloud")
    last_scraped = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "bse_announcements_aggregate"
        ordering = ["-last_scraped"]
        indexes = [
            models.Index(fields=["bse_code"]),
            models.Index(fields=["company_name"]),
            models.Index(fields=["last_scraped"]),
            models.Index(fields=["scrape_start_date", "scrape_end_date"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["bse_code", "scrape_start_date", "scrape_end_date"],
                name="uniq_bse_ann_code_dates",
            )
        ]
    
    def __str__(self):
        return f"{self.company_name or 'Unknown'} ({self.bse_code or 'No BSE Code'}) - {self.total_announcements_count} announcements"
    
    def save(self, *args, **kwargs):
        # Auto-calculate counts when saving
        if self.announcements_data and isinstance(self.announcements_data, list):
            self.total_announcements_count = len(self.announcements_data)
        
        if self.pdfs_data and isinstance(self.pdfs_data, list):
            self.total_pdfs_count = len(self.pdfs_data)
        
        super().save(*args, **kwargs)


class BseStockQuote(models.Model):
    id = models.BigAutoField(primary_key=True)
    
    # Core identifiers
    scripcode = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    
    # Scraped data fields
    security_name = models.CharField(max_length=255, null=True, blank=True)
    basic_industry = models.CharField(max_length=255, null=True, blank=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)
    
    # Metadata
    scraped_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)  # Store any scraping errors
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = "bse_stock_quotes"
        ordering = ["-scraped_at"]
        indexes = [
            models.Index(fields=["scripcode"]),
            models.Index(fields=["security_name"]),
            models.Index(fields=["basic_industry"]),
            models.Index(fields=["scraped_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["scripcode"],
                name="uniq_bse_stock_scripcode",
            )
        ]
    
    def __str__(self):
        return f"{self.security_name or 'Unknown'} ({self.scripcode or 'No Code'}) - {self.basic_industry or 'No Industry'}"
    
    @property
    def has_error(self):
        """Check if this record has any scraping errors"""
        return bool(self.error_message)
    
    @property
    def is_complete(self):
        """Check if all required fields are populated"""
        return bool(self.security_name and self.basic_industry)
    

from django.db import models


class NseStockQuote(models.Model):
    id = models.BigAutoField(primary_key=True)

    # Core fields
    symbol = models.CharField(max_length=32, null=True, blank=True, db_index=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)
    basic_industry = models.CharField(max_length=255, null=True, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "nse_stock_quotes"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["symbol"]),
            models.Index(fields=["company_name"]),
            models.Index(fields=["basic_industry"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["symbol"],
                name="uniq_nse_stock_symbol",
            )
        ]

    def __str__(self):
        return f"{self.company_name or 'Unknown'} ({self.symbol or 'No Symbol'}) - {self.basic_industry or 'No Industry'}"
    


    from django.db import models

class NSECorporateAction(models.Model):
    id = models.BigAutoField(primary_key=True)
    
    # Company details
    symbol = models.CharField(max_length=50, unique=True, db_index=True, help_text="NSE stock symbol")
    company_name = models.CharField(max_length=255, null=True, blank=True, help_text="Company name")
    
    # Corporate actions data (JSON field)
    actions_data = models.JSONField(null=True, blank=True, help_text="JSON object with equity and sme corporate actions")
    
    # File storage details
    json_r2_path = models.TextField(null=True, blank=True, help_text="R2 object key path for JSON file")
    json_cloud_url = models.TextField(null=True, blank=True, help_text="R2 public URL for JSON file")
    
    # Metadata
    total_actions_count = models.IntegerField(default=0, help_text="Total number of corporate actions (equity + sme)")
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "nse_corporate_actions"
        ordering = ["-last_updated"]
        indexes = [
            models.Index(fields=["symbol"]),
            models.Index(fields=["company_name"]),
            models.Index(fields=["last_updated"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["symbol"],
                name="uniq_nse_corp_action_symbol",
            )
        ]
    
    def __str__(self):
        return f"{self.company_name or 'Unknown'} ({self.symbol or 'No Symbol'}) - {self.total_actions_count} actions"
    
    def save(self, *args, **kwargs):
        # Auto-calculate total_actions_count when saving
        if self.actions_data and isinstance(self.actions_data, dict):
            equity_count = len(self.actions_data.get("equity", [])) if self.actions_data.get("equity") else 0
            sme_count = len(self.actions_data.get("sme", [])) if self.actions_data.get("sme") else 0
            self.total_actions_count = equity_count + sme_count
        super().save(*args, **kwargs)
