from rest_framework import serializers
from .models import SeleniumAnnouncement

class SeleniumAnnouncementSerializer(serializers.ModelSerializer):
    class Meta:
        model = SeleniumAnnouncement
        fields = "__all__"
