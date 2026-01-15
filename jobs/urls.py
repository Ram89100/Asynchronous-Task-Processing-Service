from django.urls import path
from .views import JobListView, JobDetailView

urlpatterns = [
    path("", JobListView.as_view()),
    path("<str:task_id>/", JobDetailView.as_view()),
]
