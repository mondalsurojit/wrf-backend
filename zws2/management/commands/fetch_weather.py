# management/commands/fetch_weather.py
import requests
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from zws2.models import City, Station, WeatherData
from decimal import Decimal
import time


class Command(BaseCommand):
    help = 'Fetch weather data for all stations'
    CITIES = ["Hyderabad"]  # Change to [] or ["all"] for all cities

    def handle(self, *args, **options):
        self.stdout.write('Starting weather data fetch...')
        
        # Get stations to fetch data for
        if not self.CITIES or "all" in self.CITIES:
            stations = Station.objects.filter(is_active=True)
            self.stdout.write(f'Fetching for ALL cities')
        else:
            stations = Station.objects.filter(
                city__name__in=self.CITIES, is_active=True
            )
            self.stdout.write(f'Fetching for cities: {", ".join(self.CITIES)}')
        
        total_stations = stations.count()
        successful_fetches = 0
        failed_fetches = 0
        
        self.stdout.write(f'Found {total_stations} stations to process')
        
        for station in stations:
            try:
                success = self.fetch_station_data(station)
                if success: successful_fetches += 1
                else: failed_fetches += 1
            
                time.sleep(0.1) 
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error fetching {station.locality_id}: {str(e)}')
                )
                failed_fetches += 1
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Fetch complete:\n'
                f'Successful: {successful_fetches}\n'
                f'Failed: {failed_fetches}\n'
                f'Total: {total_stations}'
            )
        )
    
    def fetch_station_data(self, station):
        """Fetch weather data for a single station"""
        url = f"{settings.ZWS_API_BASE_URL}/get_locality_weather_data"
        params = {'locality_id': station.locality_id}
        headers = {
            'x-zomato-api-key': settings.ZWS_API_KEY,
            'Content-Type': 'application/json',
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == '200' and 'locality_weather_data' in data:
                weather_data = data['locality_weather_data']
                
                WeatherData.objects.create(
                    locality_id=station,
                    temperature=self.safe_decimal(weather_data.get('temperature')),
                    humidity=self.safe_decimal(weather_data.get('humidity')),
                    wind_speed=self.safe_decimal(weather_data.get('wind_speed')),
                    wind_direction=self.safe_decimal(weather_data.get('wind_direction')),
                    rain_intensity=self.safe_decimal(weather_data.get('rain_intensity')),
                    rain_accumulation=self.safe_decimal(weather_data.get('rain_accumulation')),
                    aqi_pm_10=self.safe_decimal(weather_data.get('aqi_pm_10')),
                    aqi_pm_2_5=self.safe_decimal(weather_data.get('aqi_pm_2_point_5')),
                    fetched_at=timezone.now()
                )
                
                self.stdout.write(f'✓ {station.locality_name}')
                return True
            else:
                self.stdout.write(f'✗ {station.locality_name}: Invalid response')
                return False
                
        except requests.RequestException as e:
            self.stdout.write(f'✗ {station.locality_name}: {str(e)}')
            return False

    def safe_decimal(self, value):
        """Convert value to Decimal, return None if invalid"""
        if value is None: return None
        try: return Decimal(str(value))
        except: return None