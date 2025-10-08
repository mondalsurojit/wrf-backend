from django.urls import path
from . import views

urlpatterns = [
    path('zws/', views.weather_api, name='weather_api'),
]

# /zws/?locality_id=ZWL008599&source=live
# /zws/?latitude=17.598038&longitude=78.091346&source=live

# /zws/?locality_id=ZWL008599
# /zws/?latitude=17.598038&longitude=78.091346
# /zws/?city=hyderabad

