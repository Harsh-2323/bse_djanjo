from rest_framework import serializers
from .models import SeleniumAnnouncement

class SeleniumAnnouncementSerializer(serializers.ModelSerializer):
    class Meta:
        model = SeleniumAnnouncement
        fields = "__all__"


from rest_framework import serializers
from .models import BseStockQuote

class BseStockQuoteSerializer(serializers.ModelSerializer):
    class Meta:
        model = BseStockQuote
        fields = ['id', 'scripcode', 'security_name', 'basic_industry', 'scraped_at', 'error_message']