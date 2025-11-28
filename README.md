
THIS IS A SAMPLE RESPONSE FROM THE BACKEND:

```json
{
  "grid_info": {
    "corner": [16.67005157470703, 77.75870513916016],
    "size": [147, 156],
    "steps": [0.008996989578008652, 0.009424910880625248]
  },
  "metadata": {
    "batch_info": {
      "batch_number": 1,
      "total_batches": 16,
      "batch_size": 5
    },
    "initial_timestamp": "2025-05-14_00:00:00",
    "final_timestamp": "2025-05-17_05:00:00",
    "points_per_time": 22932,
    "total_timestamps": 78,
    "variable_scales": {
      "ALBEDO": 10000,
      "EMISS": 10000,
      "PBLH": 10,
      "RH": 100,
      "SST": 100,
      "T2": 100,
      "TOTAL_RAIN": 100,
      "TSK": 100,
      "VEGFRA": 10000,
      "WIND": 100
    }
  },
  "time_series": [
    {
      "time": 0,
      "variables": {
        "ALBEDO": [1700, 1700, 2000, ...],
        "EMISS": [9850, 9850, 9500, ...],
        "PBLH": [0, 0, 0, ...],
        "RH": [0, 0, 0, ...],
        "SST": [0, 0, 0, ...],
        "T2": [0, 0, 0, ...],
        "TOTAL_RAIN": [0, 0, 0, ...],
        "TSK": [0, 0, 0, ...],
        "VEGFRA": [0, 0, 0, ...],
        "WIND": {
           direction: [9, 3, 1, ...],
           speed: [9, 3, 1, ...]
        }
      }
    },
    {
      "time": 1,
      "variables": {
        "ALBEDO": [1701, 1702, 1703, ...],
        "EMISS": [9851, 9852, 9853, ...],
        "PBLH": [1, 1, 1, ...]
        // Other variables omitted for brevity
      }
    },
    // Times 2 to 5 with similar structures...
    {
      "time": 2,
      "variables": {
        // Sample values ...
      }
    },
    ...
  ]
}
```


THIS IS A SAMPLE API RESPONSE FROM BACKEND:

```json
{
  "status": "success",
  "city": "Hyderabad",
  "fetched_at": "2025-06-17T10:48:32.646590+00:00",
  "stations": [
    {
      "station": {
        "locality_id": "ZWL003370",
        "locality_name": "Nagole",
        "latitude": 17.359969,
        "longitude": 78.565724
      },
      "locality_weather_data": {
        "temperature": 32.86,
        "humidity": 57.04,
        "wind_speed": 2.95,
        "wind_direction": 106.4
      }
    },
```


AND THIS IS STORED CITY DATA IN DATA/ZWS2.JSON ALONG WITH STATIONS, IN THE FRONTEND:


```json
{
  "Delhi NCR": [
    {
      "Id": "ZWL005764",
      "locality": "Sarita Vihar",
      "lat": 28.531759,
      "long": 77.293973,
      "device_type": "1"
    },
    {
      "Id": "ZWL008752",
      "locality": "Faridabad Sector 41-50",
      "lat": 28.460895,
      "long": 77.304764,
      "device_type": "1"
    },

```

### Manual Approach:
###### Terminal 1: redis-server                           
###### Terminal 2: celery -A met_api_server worker -l info    
###### Terminal 3: celery -A met_api_server beat -l info    
###### Terminal 4: python manage.py runserver OR waitress-serve --host=0.0.0.0 --port=8000 met_api_server.wsgi:application        

###### cloudflared tunnel --url http://localhost:8000 --protocol http2


### Using Docker:









## Documentation

---

### `get_dataset()`

Prompts user for a NetCDF file path, opens it, and returns the dataset.

* Uses a hardcoded default path if no input is provided.
* Opens file in read mode via `netCDF4.Dataset`.
* Prints path on success; exits on failure with error message.

---

### `extract_var_from_dataset(dataset, var_name, time_idx=None)`

* Returns variable `var_name` from NetCDF `dataset`.
* If scalar → returns `var_data[:]`.
* If `time_idx` is specified and "time" is a dimension → returns data at `time_idx`.
* If `time_idx` is `None` → returns full variable.
* Returns `None` if variable not found or on error.

---

### `get_time_info(dataset)`

Extracts time step information from a NetCDF dataset.

* Attempts to read `XTIME` variable (supports 1D and 2D formats).
* If `XTIME` is missing, infers time steps from the first dimension of `T2`, `U10`, or `V10`.
* Returns a list of time values or indices.
* Defaults to `[0]` if time information is unavailable or on error.

---


"""
2D surface variables (excluding time): T2 (2-meter air temperature), TSK (skin temperature), SST (sea surface temperature), RAINC (convective precipitation), RAINNC (non-convective precipitation), ALBEDO (surface albedo), VEGFRA (vegetation fraction), EMISS (surface emissivity), PBLH (planetary boundary layer height), RH (relative humidity), TOTAL_RAIN (total precipitation), WIND (wind speed).

3D atmospheric variables (excluding time): P (pressure), QVAPOR (specific humidity), CLDFRA (cloud fraction), TKE_PBL (turbulent kinetic energy in the planetary boundary layer).
"""