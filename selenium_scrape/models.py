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
