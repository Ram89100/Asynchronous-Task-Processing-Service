# myproject/celery.py
from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'selteq_task.settings')

app = Celery('selteq_task')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related config keys should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Limit the number of broker connections Celery will open concurrently.
# This helps avoid exhausting Redis `maxclients` under high test concurrency.
import os
try:
    broker_pool_limit = int(os.getenv('CELERY_BROKER_POOL_LIMIT', '3'))
except Exception:
    broker_pool_limit = 3
app.conf.broker_pool_limit = broker_pool_limit

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))

app.conf.beat_schedule = {
    'print-task-details-every-minute': {
        'task': 'tasks.tasks.print_task_details',  # Reference the task you created
        'schedule': crontab(minute='*/1'),  # Run every minute
    },
}
