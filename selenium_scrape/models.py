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


class NseAnnouncement(models.Model):
    id = models.BigAutoField(primary_key=True)

    # Core identifiers
    symbol = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)

    # Text / subject
    subject = models.TextField(null=True, blank=True)

    # Timestamps
    exchange_received_time = models.CharField(max_length=40, null=True, blank=True)
    exchange_dissemination_time = models.CharField(max_length=40, null=True, blank=True)
    time_taken = models.CharField(max_length=20, null=True, blank=True)

    # Attachments
    attachment_size = models.CharField(max_length=50, null=True, blank=True)
    attachment_link = models.TextField(null=True, blank=True)   # may contain multiple PDFs
    xbrl_link = models.TextField(null=True, blank=True)         # may contain multiple XBRLs
    has_xbrl = models.BooleanField(default=False)

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
            models.Index(fields=["symbol", "exchange_dissemination_time"]),
            models.Index(fields=["xbrl_category"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["symbol", "subject", "exchange_dissemination_time"],
                name="uniq_nse_ann_symbol_subject_time",
            )
        ]

    def __str__(self):
        txt = self.subject or self.xbrl_subject or ""
        return f"{self.symbol or self.company_name or ''} - {txt[:60]}"
