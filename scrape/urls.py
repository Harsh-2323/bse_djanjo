from django.urls import path
from .views import RunSpiderView, RunSpiderView2

urlpatterns = [
    path("run-spider/", RunSpiderView.as_view(), name="run-spider"),
    path("run-spider2/", RunSpiderView2.as_view(), name="run-spider2"),  # new
]
