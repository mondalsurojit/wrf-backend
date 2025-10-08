from django.db import models


class City(models.Model):
    name = models.CharField(max_length=100, unique=True)
    
    def __str__(self): return self.name
    
    class Meta:
        verbose_name_plural = "Cities"


class Station(models.Model):
    locality_id = models.CharField(max_length=20, primary_key=True)
    city = models.ForeignKey(City, on_delete=models.CASCADE, related_name='stations')
    locality_name = models.CharField(max_length=200)
    latitude = models.DecimalField(max_digits=10, decimal_places=8)
    longitude = models.DecimalField(max_digits=11, decimal_places=8)
    device_type = models.CharField(max_length=50)
    is_active = models.BooleanField(default=True)
    
    def __str__(self):
        return f"{self.locality_name} ({self.locality_id})"
    
    class Meta:
        indexes = [
            models.Index(fields=['latitude', 'longitude']),
        ]


class WeatherData(models.Model):
   locality_id = models.ForeignKey(Station, on_delete=models.CASCADE, related_name='weather_data', db_column='locality_id')
   temperature = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
   humidity = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
   wind_speed = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
   wind_direction = models.DecimalField(max_digits=5, decimal_places=1, null=True, blank=True)
   rain_intensity = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
   rain_accumulation = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
   aqi_pm_10 = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
   aqi_pm_2_5 = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
   fetched_at = models.DateTimeField()
   
   def save(self, *args, **kwargs):
       if self.fetched_at:
           self.fetched_at = self.fetched_at.replace(second=0, microsecond=0)
       super().save(*args, **kwargs)
   
   def __str__(self):
       return f"{self.locality_id.locality_name} - {self.fetched_at}"
   
   class Meta:
       indexes = [
           models.Index(fields=['locality_id', 'fetched_at']),
           models.Index(fields=['fetched_at']),
       ]
       ordering = ['-fetched_at']