from celery import Celery
import os

# Set Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'met_api_server.settings')

# Create Celery app
app = Celery('weather_system')

# Load configuration from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from all installed apps
app.autodiscover_tasks()