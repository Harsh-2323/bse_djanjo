import os
import subprocess
import tempfile
from pathlib import Path
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

ALLOWED_SPIDERS = {
    "bse_public_issues_with_detail",
    "bse_ann_api",
    "example_spider",
    "bse_ann_playwright",
    "bse_ann_html_only"
}

class RunSpiderView(APIView):
    def post(self, request):
        spider = request.data.get("spider")
        args = request.data.get("args", {})

        if spider not in ALLOWED_SPIDERS:
            return Response({"detail": "Invalid spider"}, status=status.HTTP_400_BAD_REQUEST)

        project_root = Path("D:/BSE_django/bse_api")

        cmd = ["scrapy", "crawl", spider]
        for k, v in args.items():
            cmd += ["-a", f"{k}={v}"]

        env = os.environ.copy()
        env.setdefault("SCRAPY_SETTINGS_MODULE", "bse_scraper.settings")
        env["PYTHONPATH"] = str(project_root)

        # write logs safely
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log", mode="w", encoding="utf-8", errors="ignore") as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=project_root, env=env)
            rc = proc.wait()
            log_path = log.name

        # read last part of log (ignore decoding errors)
        try:
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                tail = f.read()[-2000:]
        except Exception as e:
            tail = f"Could not read log: {e}"

        return Response({
            "spider": spider,
            "status": "success" if rc == 0 else "error",
            "return_code": rc,
            "log_file": str(log_path),
            "log_tail": tail,
        })
from rest_framework import viewsets, filters
from django_filters.rest_framework import DjangoFilterBackend
from .models import PublicIssue
from .serializers import PublicIssueSerializer

class PublicIssueViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = PublicIssue.objects.all().order_by("-start_date", "-id")
    serializer_class = PublicIssueSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter, filters.SearchFilter]

    filterset_fields = ["type_of_issue", "issue_status", "exchange_platform"]
    search_fields = ["security_name"]
    ordering_fields = ["start_date", "end_date", "price_min", "price_max"]


    # --- append this at the very end of the same file (do not edit anything above) ---

class RunSpiderView2(APIView):
    """
    POST JSON:
    {
      "spider": "bse_ann_api",
      "args": { "pages": "1", "segment": "C", "from_date": "25/08/2025", "to_date": "25/08/2025" },
      "output": "outputs/announcements.json",  // optional: relative to project root
      "overwrite": true,                       // optional: true -> -O (overwrite), false -> -o (append)
      "settings": { "LOG_LEVEL": "INFO" }      // optional: Scrapy -s overrides
    }
    """
    def post(self, request):
        spider = request.data.get("spider")
        args = request.data.get("args") or {}
        output = request.data.get("output")
        overwrite = bool(request.data.get("overwrite", True))
        settings_overrides = request.data.get("settings") or {}

        if spider not in ALLOWED_SPIDERS:
            return Response({"detail": "Invalid spider"}, status=status.HTTP_400_BAD_REQUEST)

        project_root = Path("D:/BSE_django/bse_api")

        # Use current Python to ensure venv is used (more reliable on Windows)
        import sys
        cmd = [sys.executable, "-m", "scrapy", "crawl", spider]

        # spider -a args
        for k, v in args.items():
            cmd += ["-a", f"{k}={v}"]

        # optional output file
        out_path = None
        if output:
            out_path = (project_root / output).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            cmd += (["-O", str(out_path)] if overwrite else ["-o", str(out_path)])

        # optional settings overrides
        for k, v in settings_overrides.items():
            cmd += ["-s", f"{k}={v}"]

        # environment
        env = os.environ.copy()
        env.setdefault("SCRAPY_SETTINGS_MODULE", "bse_scraper.settings")
        env["PYTHONPATH"] = str(project_root)

        # run and capture logs
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log", mode="w", encoding="utf-8", errors="ignore") as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=project_root, env=env)
            rc = proc.wait()
            log_path = log.name

        # log tail
        try:
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                tail = f.read()[-2000:]
        except Exception as e:
            tail = f"Could not read log: {e}"

        # detect latest FEEDS file if no explicit output provided
        detected_output = None
        if not out_path:
            guess_dir = project_root / "outputs"
            if guess_dir.exists():
                try:
                    candidates = sorted(
                        [p for p in guess_dir.glob("announcements-*.json")],
                        key=lambda p: p.stat().st_mtime,
                        reverse=True
                    )
                    if candidates:
                        detected_output = str(candidates[0])
                except Exception:
                    pass

        resp = {
            "spider": spider,
            "status": "success" if rc == 0 else "error",
            "return_code": rc,
            "log_file": str(log_path),
            "log_tail": tail,
        }
        if out_path:
            resp["output_file"] = str(out_path)
        elif detected_output:
            resp["output_file"] = detected_output

        return Response(resp, status=status.HTTP_200_OK if rc == 0 else status.HTTP_500_INTERNAL_SERVER_ERROR)
