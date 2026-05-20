# AsyncTaskProcessor

A Django-based async task processing system using Celery, Redis, and PostgreSQL. Demonstrates practical patterns for background job execution, including automatic retries, state tracking, and horizontal scaling.

## Overview

This project shows how to decouple request processing from task execution, allowing your API to remain responsive while handling long-running operations. Tasks are enqueued to Redis and processed by Celery workers, with results persisted to PostgreSQL.

## Problem & Solution

Without task queues, slow operations block your API:

```python
# Blocks the user for 8-18 seconds
def process_payment(request):
    charge_card()      # 5-10s
    send_email()       # 2-5s
    update_analytics() # 1-3s
    return Response()
```

With async processing, requests return immediately:

```python
# Returns in <1ms
def process_payment(request):
    task_id = charge_payment.delay(user_id=123)
    return Response({"task_id": task_id})

# Worker processes independently with automatic retries
@shared_task(autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def charge_payment(user_id):
    # Your work here
    pass
```

## System Architecture

Requests enqueue tasks to Redis, workers process them asynchronously, and results are stored in PostgreSQL:

```
Client → API (Django) → Redis Queue → Celery Workers → PostgreSQL (results)
```

Key components:
- **Django REST API**: Validates requests, enqueues tasks, serves status
- **Redis**: High-speed message broker for task queues
- **Celery Workers**: Process tasks with automatic retry logic
- **PostgreSQL**: Persists job state (PENDING, RUNNING, SUCCESS, FAILURE)
- **Celery Beat**: Optional scheduler for cron-like tasks

## What's Included

- Async task execution with automatic retries (exponential backoff)
- At-least-once delivery semantics with idempotent task patterns
- Multi-tenant isolation (user-scoped queries)
- JWT authentication with refresh tokens
- Real-time monitoring via Flower dashboard
- Docker Compose setup (PostgreSQL, Redis, pgAdmin)
- Load testing tools (Locust)
- Comprehensive API endpoints

## Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 13+
- Redis 6+
- Docker & Docker Compose (optional)

### Local Development

```bash
# 1. Clone and setup
git clone <repo>
cd async-task-processor
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Install and configure
pip install -r requirements.txt
cp .env.example .env

# 3. Start database and cache
docker-compose up -d

# 4. Run migrations
python manage.py migrate

# 5. Create superuser
python manage.py createsuperuser

# 6. Start services (3 terminals)
python manage.py runserver              # Terminal 1: Django on port 8000
celery -A selteq_task worker            # Terminal 2: Worker
celery -A selteq_task beat              # Terminal 3: Scheduler
```

### Verify It Works

```bash
# Get access token
TOKEN=$(curl -X POST http://localhost:8000/api/token/ \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"password"}' \
  | jq -r '.access')

# Create and enqueue a task
curl -X POST http://localhost:8000/api/tasks/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title":"Test Task","duration":"5"}'

# Monitor with Flower
celery -A selteq_task flower --port=5555
# Open http://localhost:5555
```

## API Endpoints

All endpoints require JWT authentication (access token expires in 5 minutes).

### Authentication

```http
POST /api/token/
Content-Type: application/json

{
  "username": "user@example.com",
  "password": "password"
}

Response:
{
  "access": "eyJ...",
  "refresh": "eyJ..."
}
```

### Task Management

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/tasks/` | POST | Create task and enqueue async work |
| `/api/tasks/list/` | GET | List user's recent tasks |
| `/api/tasks/{id}/` | GET | Retrieve specific task |
| `/api/tasks/update/{id}/` | PUT | Update task title |
| `/api/tasks/delete/{id}/` | DELETE | Delete task |

### Job Status Tracking

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/jobs/{task_id}/` | GET | Poll job status and results |

**Status values**: PENDING, RUNNING, SUCCESS, FAILURE, RETRY

Example:
```http
POST /api/tasks/
Authorization: Bearer {token}
Content-Type: application/json

{
  "title": "Process Invoice",
  "duration": "5"
}

Response:
{
  "id": 42,
  "job_id": "abc-def-123-456",
  "status": "PENDING"
}
```

## Task Execution

Tasks are defined using the `@shared_task` decorator and automatically retry on failure:

```python
@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def long_running_task(self, task_id, user_id):
    job, _ = Job.objects.get_or_create(
        task_id=self.request.id,
        defaults={"status": "PENDING"}
    )
    
    try:
        # Do your work here
        result = process_data(task_id, user_id)
        
        job.status = "SUCCESS"
        job.result = {"data": result}
        job.save()
        
        return result
    except Exception as e:
        job.status = "FAILURE"
        job.result = {"error": str(e)}
        job.save()
        raise
```

Enqueue from your view:

```python
async_result = long_running_task.apply_async(
    args=[task_pk, user_id],
    ignore_result=True
)
```

## Scheduled Tasks

Configure recurring tasks in settings:

```python
CELERY_BEAT_SCHEDULE = {
    'cleanup-every-day': {
        'task': 'tasks.tasks.cleanup_old_jobs',
        'schedule': crontab(hour=2, minute=0),  # 2 AM daily
    },
}
```

## Features

- **Automatic Retries**: Exponential backoff up to 3 times by default
- **Idempotent Execution**: Same task_id won't create duplicate work
- **State Tracking**: Full ACID compliance for job status
- **Multi-Tenant**: User-scoped queries with foreign key isolation
- **Monitoring**: Real-time dashboard via Celery Flower
- **Scaling**: Add workers horizontally to increase throughput

## Deployment

### Docker Compose

```bash
# Start services (PostgreSQL, pgAdmin, Redis)
docker-compose up -d

# Run migrations
python manage.py migrate

# Start services locally
python manage.py runserver
celery -A selteq_task worker -l info
celery -A selteq_task beat -l info
```

**Services**:
- PostgreSQL 15 (port 5432)
- pgAdmin (port 5050)
- Redis (message broker)
- Django API (port 8000)

### Production Checklist

Before deploying to production:

- Set DEBUG = False
- Use environment variables for SECRET_KEY and database credentials
- Enable HTTPS and security headers (HSTS, X-Frame-Options)
- Use managed database and Redis services (AWS RDS, ElastiCache, etc.)
- Implement rate limiting
- Set up centralized logging (Datadog, CloudWatch, ELK)
- Configure alerts for queue depth, error rates, and latency
- Enable Redis persistence (AOF or RDB)
- Test database backups and recovery procedures
- Implement Beat scheduler HA or use external scheduler

## Performance

### Bottlenecks and Optimization

| Bottleneck | Symptom | Fix |
|------------|---------|-----|
| Django workers exhausted | Increased API latency | Add more worker processes |
| PostgreSQL connection pool | Database timeouts | Increase CONN_MAX_AGE, add PgBouncer |
| Redis throughput | Task enqueue slow | Use Redis cluster or sharding |
| Task payload size | Slow serialization | Offload large data to S3 |

### Load Testing

Run Locust tests to profile your deployment:

```bash
# Light load (10 concurrent users, 2 minutes)
locust -f load_tests/locustfile.py -u 10 -r 1 -t 2m --headless

# Analyze results
python -c "
import pandas as pd
df = pd.read_csv('sync_test_stats.csv')
print(f'p99 latency: {df[\"99%\"].max()}ms')
"
```

## Troubleshooting

### Queue stuck with pending tasks

```bash
# Check if workers are running
celery -A selteq_task inspect active

# Restart worker
celery -A selteq_task worker --pool=solo
```

### Task timing out

```bash
# Increase time limit
CELERY_TASK_TIME_LIMIT = 600  # 10 minutes
```

### PostgreSQL connection exhausted

```bash
# Check active connections
SELECT count(*) FROM pg_stat_activity;

# Increase pool size
DATABASES['default']['CONN_MAX_AGE'] = 600
```

### High memory usage

```bash
# Reduce prefetch multiplier
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
```