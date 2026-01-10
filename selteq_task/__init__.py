from __future__ import absolute_import, unicode_literals

# Import Celery app lazily so Django management commands don't fail
# when `celery` isn't installed in the environment.
try:
	from .celery import app as celery_app
except Exception:
	celery_app = None

__all__ = ('celery_app',)
