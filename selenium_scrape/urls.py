from django.urls import path, include
from django.shortcuts import redirect
from rest_framework.routers import DefaultRouter
from selenium_scrape.views import SeleniumAnnouncementViewSet, announcements_page

router = DefaultRouter()
router.register("selenium-announcements", SeleniumAnnouncementViewSet, basename="selenium-announcements")

urlpatterns = [
    path("", lambda request: redirect("announcements/")),  # redirect root to announcements
    path("announcements/", announcements_page, name="announcements-page"),
    path("api/", include(router.urls)),
]
