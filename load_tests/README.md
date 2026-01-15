Locust load tests for AsyncTaskProcessor

Quick setup
- Install locust (you said it's already installed):

```bash
pip install locust
```

Environment variables (set before running)
- `BASE_PATH` — base prefix (default: `/`).
- `SYNC_PATH` — sync API path (default: `/api/sync/`).
- `ASYNC_SUBMIT_PATH` — async submit path (default: `/api/async/submit/`).
- `ASYNC_STATUS_PATH` — status path with `{job_id}` (default: `/api/async/status/{job_id}/`).
- `AUTH_HEADER` — optional `Authorization` header value.
- `POLL_INTERVAL`, `POLL_TIMEOUT` — polling config for submit-and-poll task.

Running examples (headless)

# 1) Measure API throughput (target ~150 RPS):
locust -f load_tests/locustfile.py --headless -u 300 -r 100 -t 2m --csv=sync_test --only-summary

# 2) Measure async submission rate (many submits/sec):
locust -f load_tests/locustfile.py --headless -u 500 -r 200 -t 2m --csv=submit_test --only-summary --users 500 --headless

# 3) End-to-end processing capacity (submit+poll):
locust -f load_tests/locustfile.py --headless -u 200 -r 50 -t 5m --csv=proc_test --only-summary

Notes on metrics to collect for your resume bullets
- API Throughput (sync): use the `sync_test_stats.csv` and `sync_test_distribution.csv` from Locust. Record "Requests/s" and the P95 latency from distribution or the 95th percentile in the UI/CSV.
- Async Job Submission Rate: from `submit_test_stats.csv` record "Requests/s" for the `Async Submit` request.
- Async Job Processing Capacity: run the submit-and-poll test and count successful job completions per minute. Locust CSV `proc_test_stats.csv` shows total requests; divide number of successful `Async Poll` completions by test minutes to compute jobs/min.

Tips
- Use `--csv` to save results and `--only-summary` to avoid interactive UI.
- Increase load gradually and monitor your Redis/Celery workers (use Flower or `htop`) to avoid overload.
- For precise job-completion counts, have your status endpoint return explicit `status` and `job_id` fields as JSON.
