from django.db import models

class PublicIssue(models.Model):
    id = models.BigAutoField(primary_key=True)
    security_name = models.CharField(max_length=512)
    exchange_platform = models.CharField(max_length=128, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    offer_price = models.CharField(max_length=64, null=True, blank=True)
    face_value = models.CharField(max_length=64, null=True, blank=True)
    type_of_issue = models.CharField(max_length=64, null=True, blank=True)
    issue_status = models.CharField(max_length=64, null=True, blank=True)
    price_min = models.FloatField(null=True, blank=True)
    price_max = models.FloatField(null=True, blank=True)
    type_of_issue_long = models.CharField(max_length=128, null=True, blank=True)
    list_url = models.TextField(null=True, blank=True)
    detail_url = models.TextField(null=True, blank=True)

    details = models.JSONField(default=dict, blank=True)
    pdf_links = models.JSONField(default=list, blank=True)
    links = models.JSONField(default=list, blank=True)
    documents = models.JSONField(default=list, blank=True)
    file_urls = models.JSONField(default=list, blank=True)
    uploaded_files = models.JSONField(default=list, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "public_issues"
        indexes = [models.Index(fields=["start_date", "end_date"])]

    def __str__(self):
        return f"{self.security_name or ''} ({self.start_date} → {self.end_date})"


class Announcement(models.Model):
    news_id = models.CharField(max_length=50, primary_key=True)
    scrip_cd = models.CharField(max_length=20, null=True, blank=True)
    company_name = models.TextField(null=True, blank=True)

    segment = models.CharField(max_length=10, null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    subcategory = models.CharField(max_length=255, null=True, blank=True)

    headline = models.TextField(null=True, blank=True)
    body_html = models.TextField(null=True, blank=True)
    body_text = models.TextField(null=True, blank=True)

    dissem_dt_ist = models.DateTimeField(null=True, blank=True)
    dissem_dt_utc = models.DateTimeField(null=True, blank=True)
    received_dt_ist = models.DateTimeField(null=True, blank=True)
    time_taken_sec = models.IntegerField(null=True, blank=True)

    # declared links on the API item
    pdf_url = models.TextField(null=True, blank=True)
    pdf_size_bytes = models.BigIntegerField(null=True, blank=True)
    xbrl_url = models.TextField(null=True, blank=True)
    av_url = models.TextField(null=True, blank=True)

    has_pdf = models.BooleanField(default=False)
    has_xbrl = models.BooleanField(default=False)
    has_av = models.BooleanField(default=False)

    is_revision = models.BooleanField(default=False)
    reg_tags = models.JSONField(default=list, blank=True)  # e.g. ["Reg 30"]

    company_url = models.TextField(null=True, blank=True)
    source_url = models.TextField(null=True, blank=True)
    page_no = models.IntegerField(null=True, blank=True)

    # keep the raw item for auditing/future-proofing
    payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "announcements"
        ordering = ["-dissem_dt_ist"]

    def __str__(self):
        return f"{self.news_id} – {self.headline or ''}"


class AnnouncementAttachment(models.Model):
    KIND_CHOICES = (("pdf", "PDF"), ("xbrl", "XBRL"), ("audio_video", "Audio/Video"))

    id = models.BigAutoField(primary_key=True)
    announcement = models.ForeignKey(
        Announcement, on_delete=models.CASCADE, related_name="attachments"
    )
    url = models.TextField()                      # original URL (e.g., BSE pdf)
    kind = models.CharField(max_length=20, choices=KIND_CHOICES)

    size_bytes = models.BigIntegerField(null=True, blank=True)
    mime = models.CharField(max_length=120, null=True, blank=True)
    local_path = models.TextField(null=True, blank=True)
    sha256 = models.CharField(max_length=64, null=True, blank=True)

    r2_key = models.TextField(null=True, blank=True)
    r2_url = models.TextField(null=True, blank=True)
    bucket = models.TextField(null=True, blank=True)

    text_excerpt = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "announcement_attachments"
        unique_together = ("announcement", "url")

    def __str__(self):
        return f"{self.kind} – {self.url}"