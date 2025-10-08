# management/commands/load_stations.py
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings
from zws2.models import City, Station
import os


class Command(BaseCommand):
    help = 'Load stations from Excel file'

    def handle(self, *args, **options):
        excel_path = os.path.join(settings.BASE_DIR, 'zws.xlsx')
        
        if not os.path.exists(excel_path):
            self.stdout.write(
                self.style.ERROR(f'Excel file not found: {excel_path}')
            )
            return
        
        try:
            # Read Excel file
            df = pd.read_excel(excel_path)
            
            # Clean device_type (extract number from "1 - Automated weather system")
            df['device_type_clean'] = df['device_type'].astype(str).str.extract(r'(\d+)')[0]
            
            cities_created = 0
            stations_created = 0
            stations_updated = 0
            
            for _, row in df.iterrows():
                # Create or get city
                city, created = City.objects.get_or_create(
                    name=row['cityName']
                )
                if created:
                    cities_created += 1
                
                # Create or update station
                station, created = Station.objects.update_or_create(
                    locality_id=row['localityId'],
                    defaults={
                        'city': city,
                        'locality_name': row['localityName'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'device_type': row['device_type_clean'] or '1',
                    }
                )
                
                if created: stations_created += 1
                else: stations_updated += 1
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully loaded data:\n'
                    f'Cities created: {cities_created}\n'
                    f'Stations created: {stations_created}\n'
                    f'Stations updated: {stations_updated}'
                )
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error loading data: {str(e)}')
            )