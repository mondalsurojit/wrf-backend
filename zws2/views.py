# views.py
import requests
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.cache import cache_page
from django.db.models import Q
from django.conf import settings
from zws2.models import City, Station, WeatherData
from decimal import Decimal
import re


@require_http_methods(["GET"])
def weather_api(request):
    """
    Unified weather API endpoint
    Supports:
    - zws/?latitude=...&longitude=...
    - zws/?locality_id=...
    - zws/?city=... (single, multiple comma/ampersand separated, or "all")
    - Add &source=live for live data (proxy to Weather Union API)
    """
    
    # Check if live data is requested
    source = request.GET.get('source', '').lower()
    is_live = source == 'live'
    
    if is_live: return handle_live_weather_request(request)
    else: return handle_cached_weather_request(request)


def handle_live_weather_request(request):
    """Handle live weather data requests (proxy to Weather Union API)"""
    
    latitude = request.GET.get('latitude')
    longitude = request.GET.get('longitude')
    locality_id = request.GET.get('locality_id')
    headers = {
        'x-zomato-api-key': settings.ZWS_API_KEY,
        'Content-Type': 'application/json',
    }
    
    if latitude and longitude:
        # Live weather by coordinates
        url = f"{settings.ZWS_API_BASE_URL}/get_weather_data"
        params = {
            'latitude': latitude,
            'longitude': longitude
        }
    elif locality_id:
        # Live weather by locality ID
        url = f"{settings.ZWS_API_BASE_URL}/get_locality_weather_data"
        params = {'locality_id': locality_id}
    else:
        return JsonResponse({
            'error': 'For live data, provide either latitude+longitude or locality_id'
        }, status=400)
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return JsonResponse(response.json())
    except requests.RequestException as e:
        return JsonResponse({
            'error': f'Failed to fetch live data: {str(e)}'
        }, status=500)


@cache_page(60 * 5)  # Cache for 5 minutes
def handle_cached_weather_request(request):
    """Handle cached weather data requests from database"""
    
    latitude = request.GET.get('latitude')
    longitude = request.GET.get('longitude')
    locality_id = request.GET.get('locality_id')
    city = request.GET.get('city')
    
    if latitude and longitude:
        return get_weather_by_coordinates(latitude, longitude)
    elif locality_id:
        return get_weather_by_locality_id(locality_id)
    elif city:
        return get_weather_by_city(city)
    else:
        return JsonResponse({
            'error': 'Provide latitude+longitude, locality_id, or city parameter'
        }, status=400)


def get_weather_by_coordinates(latitude, longitude):
    """Get weather data by coordinates (find nearest station)"""
    try:
        lat = Decimal(str(latitude))
        lon = Decimal(str(longitude))
        
        # Find nearest station (simple distance calculation)
        # For production, consider using PostGIS for better geo queries
        stations = Station.objects.filter(is_active=True)
        
        if not stations.exists():
            return JsonResponse({'error': 'No stations found'}, status=404)
        
        # Simple distance calculation (not highly accurate but fast)
        nearest_station = None
        min_distance = float('inf')
        
        for station in stations:
            distance = abs(station.latitude - lat) + abs(station.longitude - lon)
            if distance < min_distance:
                min_distance = distance
                nearest_station = station
        
        if nearest_station:
            latest_data = WeatherData.objects.filter(locality_id=nearest_station).order_by('-fetched_at').first()
            
            if latest_data: return JsonResponse(format_weather_response(latest_data))
            else: return JsonResponse({'error': 'No weather data found'}, status=404)
        
        return JsonResponse({'error': 'No nearby stations found'}, status=404)
        
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Invalid coordinates'}, status=400)


def get_weather_by_locality_id(locality_id):
    """Get weather data by locality ID"""
    try:
        station = Station.objects.get(locality_id=locality_id, is_active=True)
        latest_data = WeatherData.objects.filter(locality_id=station).order_by('-fetched_at').first()
        
        if latest_data: return JsonResponse(format_weather_response(latest_data))
        else: return JsonResponse({'error': 'No weather data found'}, status=404)
            
    except Station.DoesNotExist: return JsonResponse({'error': 'Station not found'}, status=404)


def get_weather_by_city(city_param):
    """Get weather data by city name(s)"""
    
    if city_param.lower() == 'all':
        # Get all cities
        cities = City.objects.all()
    else:
        # Parse city names (comma or ampersand separated)
        city_names = re.split(r'[,&]', city_param)
        city_names = [name.strip() for name in city_names if name.strip()]
        
        cities = City.objects.filter(name__in=city_names)
        
        if not cities.exists(): return JsonResponse({'error': 'No cities found'}, status=404)
    
    # Handle single city vs multiple cities response format
    if len(cities) == 1 and city_param.lower() != 'all':
        # Single city format
        city = cities.first()
        stations = city.stations.filter(is_active=True)
        
        city_stations = []
        latest_fetched_at = None
        
        for station in stations:
            latest_data = WeatherData.objects.filter(locality_id=station).order_by('-fetched_at').first()
            if latest_data:
                station_data = {
                    'station': {
                        'locality_id': station.locality_id,
                        'locality_name': station.locality_name,
                        'latitude': float(station.latitude),
                        'longitude': float(station.longitude)
                    },
                    'locality_weather_data': {
                        'temperature': float(latest_data.temperature) if latest_data.temperature else None,
                        'humidity': float(latest_data.humidity) if latest_data.humidity else None,
                        'wind_speed': float(latest_data.wind_speed) if latest_data.wind_speed else None,
                        'wind_direction': float(latest_data.wind_direction) if latest_data.wind_direction else None,
                        'rain_intensity': float(latest_data.rain_intensity) if latest_data.rain_intensity else None,
                        'rain_accumulation': float(latest_data.rain_accumulation) if latest_data.rain_accumulation else None,
                        'aqi_pm_10': float(latest_data.aqi_pm_10) if latest_data.aqi_pm_10 else None,
                        'aqi_pm_2_point_5': float(latest_data.aqi_pm_2_5) if latest_data.aqi_pm_2_5 else None,
                    }
                }
                
                # Remove None values from locality_weather_data
                station_data['locality_weather_data'] = {
                    k: v for k, v in station_data['locality_weather_data'].items() if v is not None
                }
                
                city_stations.append(station_data)
                
                # Track the latest fetched_at time
                if latest_fetched_at is None or latest_data.fetched_at > latest_fetched_at:
                    latest_fetched_at = latest_data.fetched_at
        
        if city_stations:
            return JsonResponse({
                'status': 'success',
                'city': city.name,
                'fetched_at': latest_fetched_at.isoformat(),
                'stations': city_stations
            })
        else:
            return JsonResponse({'error': 'No weather data found'}, status=404)
    
    else:
        # Multiple cities format (existing logic)
        weather_data = []
        
        for city in cities:
            stations = city.stations.filter(is_active=True)
            
            city_weather = {
                'city': city.name,
                'stations': []
            }
            
            for station in stations:
                latest_data = WeatherData.objects.filter(locality_id=station).order_by('-fetched_at').first()
                if latest_data:
                    station_data = format_weather_response(latest_data)
                    station_data['station_info'] = {
                        'locality_name': station.locality_name,
                        'locality_id': station.locality_id,
                        'latitude': float(station.latitude),
                        'longitude': float(station.longitude)
                    }
                    city_weather['stations'].append(station_data)
            
            if city_weather['stations']:  # Only add cities with data
                weather_data.append(city_weather)
        
        if weather_data:
            return JsonResponse({
                'status': 'success',
                'data': weather_data,
                'total_cities': len(weather_data)
            })
        else:
            return JsonResponse({'error': 'No weather data found'}, status=404)


def format_weather_response(weather_data):
    """Format weather data for consistent API response"""
    return {
        'status': '200',
        'locality_weather_data': {
            'temperature': float(weather_data.temperature) if weather_data.temperature else None,
            'humidity': float(weather_data.humidity) if weather_data.humidity else None,
            'wind_speed': float(weather_data.wind_speed) if weather_data.wind_speed else None,
            'wind_direction': float(weather_data.wind_direction) if weather_data.wind_direction else None,
            'rain_intensity': float(weather_data.rain_intensity) if weather_data.rain_intensity else None,
            'rain_accumulation': float(weather_data.rain_accumulation) if weather_data.rain_accumulation else None,
            'aqi_pm_10': float(weather_data.aqi_pm_10) if weather_data.aqi_pm_10 else None,
            'aqi_pm_2_point_5': float(weather_data.aqi_pm_2_5) if weather_data.aqi_pm_2_5 else None,
        },
        'fetched_at': weather_data.fetched_at.isoformat(),
        'station': {
            'locality_id': weather_data.locality_id.locality_id,
            'locality_name': weather_data.locality_id.locality_name,  
            'city': weather_data.locality_id.city.name,
            'latitude': float(weather_data.locality_id.latitude),
            'longitude': float(weather_data.locality_id.longitude)
        }
    }