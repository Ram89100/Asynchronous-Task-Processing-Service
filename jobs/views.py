from rest_framework import generics
from .models import Job
from .serializers import JobSerializer

class JobListView(generics.ListAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer

class JobDetailView(generics.RetrieveAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    lookup_field = "task_id"
from django.shortcuts import render

# Create your views here.
