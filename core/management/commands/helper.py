import os, json, gzip
import numpy as np
from netCDF4 import Dataset
from typing import Dict, List, Tuple, Any
from skimage.measure import block_reduce


def get_dataset(file_path):
    """
    Open the dataset from the given file_path.
    Raises an error if file_path is missing or invalid.
    """
    if not file_path:
        raise ValueError("File path is required. Please provide a dataset file path.")

    try:
        dataset = Dataset(file_path, 'r')
        print(f"Opened dataset: {file_path}")
        return dataset
    except Exception as e:
        raise RuntimeError(f"Error opening dataset: {e}")


def get_time_info(dataset):
    """Extract time information from the dataset - returns both numerical times and timestamps"""
    try:
        # Extract numerical time values - access XTIME directly since it's 1D
        xtime = None
        if 'XTIME' in dataset.variables:
            try:
                xtime_var = dataset.variables['XTIME']
                xtime_values = xtime_var[:].tolist()
                # Convert to integer and divide by 60 to get hourly indices
                xtime = [int(val) // 60 for val in xtime_values]
            except Exception as e:
                print(f"XTIME access failed: {e}")
                xtime = None
        
        if xtime is not None: times = xtime
        else:
            print("Warning: Could not determine time information, using single time step")
            times = [0]
            
        # Extract timestamps directly from Times variable
        timestamps = None
        
        if 'Times' in dataset.variables:
            times_var = dataset.variables['Times']
            
            if len(times_var.shape) == 2:
                timestamps = []
                for i in range(times_var.shape[0]):
                    time_str = ''.join([char.decode('utf-8') if isinstance(char, bytes) else str(char) 
                                      for char in times_var[i, :] if char != b'\x00' and char != '\x00'])
                    timestamps.append(time_str.strip())
                
                print(f"Extracted {len(timestamps)} timestamps from Times variable")
        
        # Ensure timestamps list matches times list
        if timestamps is not None and len(timestamps) != len(times):
            if len(timestamps) > len(times): timestamps = timestamps[:len(times)]
            else: timestamps = None
        
        if timestamps:
            print(f"Initial timestamp: {timestamps[0]}")
            print(f"Final timestamp: {timestamps[-1]}")
        
        return times, timestamps
            
    except Exception as e:
        print(f"Error getting time info: {e}")
        return [0], None
    

def clean_invalid_values(data, var_name):
    """Clean invalid values from NetCDF data (fill values, extreme values)"""
    if data is None: return None
    
    data = np.array(data) # Convert to numpy array if not already
    
    # Define reasonable ranges for different variable types
    valid_ranges = {
        'T2': (-150, 350),      # Temperature in Celsius after conversion
        'TSK': (-150, 350),     # Skin temperature  
        'SST': (-150, 350),     # Sea surface temperature
        'U10': (-200, 200),     # Wind components
        'V10': (-200, 200),
        'P': (0, 110000),       # Pressure in Pa (100-1100 hPa)
        'QVAPOR': (0, 0.1),     # Specific humidity (0-100 g/kg)
        'RAINC': (0, 1000),     # Rain (0-1000mm)
        'RAINNC': (0, 1000),
        'PBLH': (0, 5000),      # Boundary layer height (0-5km)
        'CLDFRA': (0, 1.1),     # Cloud fraction (0-1)
        'ALBEDO': (0, 1.1),     # Albedo (0-1)
        'VEGFRA': (0, 1.1),     # Vegetation fraction (0-1)
        'EMISS': (0, 1.1),      # Emissivity (0-1)
        'TKE_PBL': (0, 100),    # Turbulent kinetic energy
    }
    
    # NetCDF often uses values like -9999, 1e+20, etc. as fill values. Handle those.
    mask_fill = (np.abs(data) > 1e10) | (data < -1e6)
    data[mask_fill] = np.nan
    
    # Apply variable-specific range filtering
    if var_name in valid_ranges:
        min_val, max_val = valid_ranges[var_name]
        mask_range = (data < min_val) | (data > max_val)
        data[mask_range] = np.nan
        
        if np.any(mask_range):
            print(f"🧹 {var_name}: Cleaned {np.sum(mask_range)} out-of-range values")

    data[np.isinf(data)] = np.nan   # Handle infinite values
    
    return data


def extract_var_slice(dataset, var_name, time_idx, surface_level=0, all_levels=False):
    """Memory-efficient extraction of specific time slice from NetCDF variable"""
    try:
        if var_name not in dataset.variables: return None
            
        var = dataset.variables[var_name]

        if len(var.shape) == 4:  # (time, level, lat, lon)
            if all_levels: data = var[time_idx, :, :, :]  # Return all levels
            else: data = var[time_idx, surface_level, :, :] # Extract only the level slice
            print(f"🤜 {var_name}: 4D slice [{time_idx}, {surface_level}, :, :] -> shape {data.shape}")
            
        elif len(var.shape) == 3:  # (time, lat, lon) 
            # Extract only the specific time slice
            data = var[time_idx, :, :]
            print(f"🤜 {var_name}: 3D slice [{time_idx}, :, :] -> shape {data.shape}")
            
        elif len(var.shape) == 2:  # (lat, lon) - time-invariant
            data = var[:, :]
            print(f"🤜 {var_name}: 2D static data -> shape {data.shape}")
            
        else:
            print(f"🤜 {var_name}: Unexpected dimensions {var.dims} with shape {var.shape}")
            return None
        
        # Convert to numpy array and clean invalid values
        data = np.array(data)
        data = clean_invalid_values(data, var_name)
        
        return data
        
    except Exception as e:
        print(f"{var_name}: Error during extraction - {e}")
        return None


def calculate_relative_humidity(dataset, time_idx=0):
    """Calculate relative humidity using WRF variables with the correct formula"""
    try:
        qvapor = extract_var_slice(dataset, 'QVAPOR', time_idx, surface_level=0)
        temperature = extract_var_slice(dataset, 'T2', time_idx, surface_level=0)
        pressure = extract_var_slice(dataset, 'PSFC', time_idx, surface_level=0)

        if qvapor is None:
            print("QVAPOR is missing — cannot compute RH.")
            return None

        elif temperature is None or pressure is None:
            print("Missing temperature or pressure. Using simplified RH calculation based on QVAPOR only.")
            max_q = np.nanmax(qvapor)
            min_q = np.nanmin(qvapor)
            if max_q > min_q: 
                return 100.0 * (qvapor - min_q) / (max_q - min_q)
            else:
                print("Invalid QVAPOR range for fallback RH calculation.")
                return None

        elif np.all(np.isnan(qvapor)) or np.all(np.isnan(temperature)) or np.all(np.isnan(pressure)):
            print("All values are NaN in one or more required variables")
            return None

        else:
            """
            Compute relative humidity using the specified WRF formula:
            rh = 1.E2 * (p*q/(q*(1.-eps) + eps))/(svp1*exp(svp2*(t-svpt0)/(T-svp3)))
            
            Where:
            - p: pressure (Pa)
            - q: specific humidity (kg/kg)
            - t: temperature (K)
            - eps: ratio of molecular weights (0.622)
            - svp1, svp2, svpt0, svp3: saturation vapor pressure constants
            """
            
            # Constants for the WRF saturation vapor pressure formula
            eps = 0.622
            svp1 = 611.2      # Pa
            svp2 = 17.67      # dimensionless
            svpt0 = 273.15    # K (reference temperature)
            svp3 = 29.65      # K
            
            # Assign variables for clarity
            p = pressure      # Surface pressure in Pa
            q = qvapor       # Specific humidity in kg/kg
            t = temperature  # Temperature in K
            
            # Calculate saturation vapor pressure
            svp = svp1 * np.exp(svp2 * (t - svpt0) / (t - svp3))
            
            # Calculate actual vapor pressure
            vapor_pressure = p * q / (q * (1. - eps) + eps)
            
            # Calculate relative humidity using the specified formula
            rh = 1.E2 * vapor_pressure / svp
            
            # Clamp to physically reasonable values (0-100%)
            rh = np.clip(rh, 0, 100)

            if np.all(np.isnan(rh)):
                print("All RH values are NaN after calculation - check input data ranges")
                return None
            else: 
                return rh

    except Exception as e:
        print(f"Error calculating relative humidity: {e}")
        return None

def calculate_total_rain(dataset, time_idx):
    """Calculate hourly precipitation from (RAINC + RAINNC)"""
    try:
        if time_idx == 0: return None  # first timestep

        rainc_now = extract_var_slice(dataset, 'RAINC', time_idx)
        rainnc_now = extract_var_slice(dataset, 'RAINNC', time_idx)
        rainc_prev = extract_var_slice(dataset, 'RAINC', time_idx - 1)
        rainnc_prev = extract_var_slice(dataset, 'RAINNC', time_idx - 1)

        if any(v is None for v in (rainc_now, rainnc_now, rainc_prev, rainnc_prev)):
            return None

        if rainc_now.shape != rainnc_now.shape or rainc_now.shape != rainc_prev.shape or rainnc_now.shape != rainnc_prev.shape:
            print(f"HOURLY_RAIN: Shape mismatch among rain components")
            return None

        hourly_rain = (rainc_now + rainnc_now) - (rainc_prev + rainnc_prev)
        hourly_rain = clean_invalid_values(hourly_rain, 'HOURLY_RAIN')

        return hourly_rain

    except Exception as e:
        print(f"TOTAL_RAIN calculation error: {e}")
        return None

def prepare_data(data, lats, lons, group_size=1):
    """Prepare and optionally downsample data arrays"""
    # Downsample if needed
    if group_size > 1:
        if data.shape[0] >= group_size and data.shape[1] >= group_size:
            # Crop to make divisible by group_size
            ny, nx = data.shape
            ny_crop = (ny // group_size) * group_size
            nx_crop = (nx // group_size) * group_size
            
            data = data[:ny_crop, :nx_crop]
            lats = lats[:ny_crop, :nx_crop]
            lons = lons[:nx_crop, :nx_crop]
            
            # Downsample
            data = block_reduce(data, block_size=(group_size, group_size), func=np.nanmean)
            lats = block_reduce(lats, block_size=(group_size, group_size), func=np.nanmean)
            lons = block_reduce(lons, block_size=(group_size, group_size), func=np.nanmean)
    
    return data, lats, lons


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy arrays and data types"""
    def default(self, obj):
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.bool_): return bool(obj)
        return super().default(obj)

def create_compressed_json(data: Dict[str, Any], output_path: str) -> int:
    """Create compressed JSON file with NumPy array support"""
    json_str = json.dumps(data, cls=NumpyEncoder, separators=(',', ':'))
    with gzip.open(output_path, 'wt', encoding='utf-8', compresslevel=9) as f:
        f.write(json_str)
    return os.path.getsize(output_path)

