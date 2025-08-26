from rest_framework import serializers
from .models import PublicIssue

class PublicIssueSerializer(serializers.ModelSerializer):
    class Meta:
        model = PublicIssue
        fields = "__all__"
