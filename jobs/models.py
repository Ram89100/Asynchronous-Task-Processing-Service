from django.db import models

class Job(models.Model):
    task_id = models.CharField(max_length=255, unique=True)
    status = models.CharField(max_length=50)
    result = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
from django.db import models

# Create your models here.
