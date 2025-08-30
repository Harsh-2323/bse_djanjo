from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create router and register both viewsets
router = DefaultRouter()
router.register(r'selenium_announcements', views.SeleniumAnnouncementViewSet)
router.register(r'bse_stock_quotes', views.BseStockQuoteViewSet)  # This line might be missing

urlpatterns = [
    # Page views
    path('announcements/', views.announcements_page, name='announcements_page'),
    
    # API endpoints - include router URLs
    path('', include(router.urls)),
]