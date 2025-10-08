#core/views.py
from django.shortcuts import render
from django.http import JsonResponse, FileResponse, Http404
from django.conf import settings

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

import os, glob
from datetime import datetime


def health_check(request): return JsonResponse({'status': 'OK'})

class ChunkedDataTransferView(APIView):
    def get_latest_folder(self):
        """
        Find the folder with the latest date. Folder format: YYYYMMDD
        """
        folders = glob.glob(os.path.join(settings.DATA_ROOT, "*"))

        if not folders:
            raise Http404("No data folders found")

        valid_folders = []
        for folder in folders:
            folder_name = os.path.basename(folder)
            try:
                folder_date = datetime.strptime(folder_name, '%Y%m%d')
                valid_folders.append((folder_date, folder))
            except ValueError: continue

        if not valid_folders:
            raise Http404("No valid data folders found")

        return max(valid_folders)[1]  # Returns folder with latest date

    def get(self, request, chunk_no):
        chunk_no = chunk_no.zfill(3)  # zero-padded to 3 digits
        print(f"Received chunk: {chunk_no}")

        if not chunk_no.isalnum():
            raise Http404("Invalid chunk number")

        latest_folder = self.get_latest_folder()
        data_path = os.path.join(latest_folder, f"{chunk_no}.json_gz")
        if not os.path.isfile(data_path):
            raise Http404("Data file not found")

        return FileResponse(
            open(data_path, 'rb'),
            content_type='application/json',
            headers={'Content-Encoding': 'gzip'}
        )


# localhost:8000/data/001

# class MVTTileView(APIView):
#     def get(self, request, timestep, z, x, y):
#         # Add some debugging
#         print(f"Received: timestep={timestep}, z={z}, x={x}, y={y}")
        
#         # Ensure z, x, y are strings for path construction
#         tile_path = os.path.join(settings.TILE_ROOT, str(timestep), str(z), str(x), f"{y}.mvt")
        
#         if not os.path.isfile(tile_path):
#             print(f"File not found: {tile_path}")
#             raise Http404("Tile not found")

#         return FileResponse(open(tile_path, 'rb'), content_type='application/vnd.mapbox-vector-tile')
    
# # localhost:8000/tiles/001/0/0/0.mvt
