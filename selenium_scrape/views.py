import subprocess, tempfile, os, sys
from pathlib import Path
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from .models import SeleniumAnnouncement
from .serializers import SeleniumAnnouncementSerializer
from django.shortcuts import render

def announcements_page(request):
    return render(request, "announcements.html")

class SeleniumAnnouncementViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SeleniumAnnouncement.objects.all().order_by("-created_at")
    serializer_class = SeleniumAnnouncementSerializer

    @action(detail=False, methods=["post"])
    def run_scraper(self, request):
        """Trigger Selenium scraper"""
        project_root = Path(settings.BASE_DIR)

        cmd = [sys.executable, "manage.py", "scrape_bse_ann_html_only", "--max-pages", "5"]

        with tempfile.NamedTemporaryFile(delete=False, suffix=".log", mode="w", encoding="utf-8") as log:
            rc = subprocess.call(cmd, stdout=log, stderr=log, cwd=project_root)
            log_path = log.name

        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            tail = f.read()[-2000:]

        return Response({
            "status": "success" if rc == 0 else "error",
            "return_code": rc,
            "log_tail": tail,
        }, status=status.HTTP_200_OK if rc == 0 else status.HTTP_500_INTERNAL_SERVER_ERROR)
