from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

urlpatterns = [
    path("", lambda r: redirect("/api/selenium_scrape/announcements/")),  # ✅ add this
    path("admin/", admin.site.urls),
    path("api/scrape/", include("scrape.urls")),
    path("api/selenium_scrape/", include("selenium_scrape.urls")),
]
