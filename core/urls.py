# core/urls.py
from django.urls import path
from . import views
from .views import ChunkedDataTransferView

urlpatterns = [
    path('api/health/', views.health_check, name='health-check'),
    path('data/<str:chunk_no>/', ChunkedDataTransferView.as_view(), name='chunked-data-transfer'),
    # path('tiles/<str:timestep>/<int:z>/<int:x>/<int:y>.mvt', MVTTileView.as_view(), name='mvt-tile'),
]
