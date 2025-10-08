import os, json
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'met_api_server.settings')
django.setup()

from zws.models import WeatherData
from zws.serializers import WeatherDataSerializer, SimpleWeatherDataSerializer

wd = WeatherData.objects.first()

serializer = WeatherDataSerializer(wd)
print(json.dumps(serializer.data, indent=2))
