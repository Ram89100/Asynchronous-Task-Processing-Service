from celery import shared_task
from django.utils import timezone
from .models import Task
from jobs.models import Job

@shared_task
def print_task_details():
    # Fetch tasks added by user with id=1
    tasks = Task.objects.filter(user_id=1)

    # Log task details
    for task in tasks:
        # This will log the task details in the Celery worker log
        print(f"Task Title: {task.title}, Duration: {task.duration}, Created At: {task.created_at}")



@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def long_running_task(self, task_pk=None):
    # Create or fetch a Job row keyed by the Celery task id
    job, _ = Job.objects.get_or_create(
        task_id=self.request.id,
        defaults={"status": "PENDING"},
    )

    try:
        # simulate work (replace with real processing)
        import time
        time.sleep(5)

        job.status = "SUCCESS"
        job.result = {"message": "Task completed", "task_pk": task_pk}
        job.save()

        return job.result
    except Exception as exc:
        job.status = "FAILURE"
        job.result = {"error": str(exc)}
        job.save()
        raise