from celery import shared_task
from django.core.management import call_command
import logging

logger = logging.getLogger(__name__)

@shared_task
def fetch_weather_data():
    """Fetch weather data every 30 minutes"""
    try:
        call_command('fetch_weather')
        logger.info("Weather data fetch completed successfully")
        return "Weather data fetch completed"
    except Exception as e:
        logger.error(f"Weather data fetch failed: {str(e)}")
        raise