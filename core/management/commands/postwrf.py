import os, time
from datetime import datetime
import numpy as np
from typing import Dict, List, Tuple, Any
from django.core.management.base import BaseCommand

from core.management.commands.helper import get_dataset, get_time_info, extract_var_slice, calculate_relative_humidity, calculate_total_rain, create_compressed_json

# Configuration
GRID_SIZE_KM = 1.0  # 1x1 km squares
OUTPUT_FORMAT = 'json_gz'  # Fixed output format

VARIABLES_TO_PROCESS = [
    # 2D variables (excluding time)
    'T2', 'TSK', 'SST', 'U10', 'V10',
    # 'ALBEDO', 'VEGFRA', 'EMISS', 'PBLH',
    
    # Calculated variables (2D)
    'RH', 'TOTAL_RAIN', 
    
    # 3D variables (excluding time) - commented out for now
    # 'P', 'QVAPOR', 'CLDFRA', 'TKE_PBL'
]

VARIABLE_METADATA = {
    'ALBEDO' : {'name': 'albedo', 'scale': 10000, 'dtype': 'uint16'},
    'CLDFRA' : {'name': 'cloud_fraction', 'scale': 10000, 'dtype': 'uint16'},
    'EMISS'  : {'name': 'surface_emissivity', 'scale': 10000, 'dtype': 'uint16'},
    'P'      : {'name': 'pressure', 'scale': 1, 'dtype': 'float32'},
    'PBLH'   : {'name': 'planetary_boundary_layer_height', 'scale': 10, 'dtype': 'uint16'},
    'QVAPOR' : {'name': 'specific_humidity', 'scale': 1000000, 'dtype': 'uint32'},
    'RAINC'  : {'name': 'convective_rain', 'scale': 100, 'dtype': 'uint16'},
    'RAINNC' : {'name': 'non_convective_rain', 'scale': 100, 'dtype': 'uint16'},
    'SST'    : {'name': 'sea_surface_temperature', 'convert_temp': True, 'scale': 100, 'dtype': 'int16'},
    'T2'     : {'name': 'temperature_2m', 'convert_temp': True, 'scale': 100, 'dtype': 'int16'},
    'TKE_PBL': {'name': 'turbulent_kinetic_energy', 'scale': 1000, 'dtype': 'uint32'},
    'TSK'    : {'name': 'skin_temperature', 'convert_temp': True, 'scale': 100, 'dtype': 'int16'},
    'U10'    : {'name': 'eastward_wind_10m', 'scale': 100, 'dtype': 'int16'},
    'V10'    : {'name': 'northward_wind_10m', 'scale': 100, 'dtype': 'int16'},
    'VEGFRA' : {'name': 'vegetation_fraction', 'scale': 10000, 'dtype': 'uint16'},
    # Custom variables
    'RH'         : {'name': 'relative_humidity', 'custom': True, 'scale': 100, 'dtype': 'uint16'},
    'TOTAL_RAIN' : {'name': 'total_precipitation', 'custom': True, 'scale': 100, 'dtype': 'uint16'},
}

def get_variable_data(dataset, var_name, time_idx, surface_level=0):
    """Get data for a variable (handles both regular and custom variables)"""
    metadata = VARIABLE_METADATA.get(var_name, {'custom': False})
    
    if metadata.get('custom', False):
        if var_name == 'RH': 
            return calculate_relative_humidity(dataset, time_idx)
        elif var_name == 'TOTAL_RAIN': 
            return calculate_total_rain(dataset, time_idx)
        else:
            print(f"Unknown custom variable: {var_name}")
            return None
    else: 
        # Check if this is a 3D variable that should return all levels
        if var_name in ['P', 'QVAPOR', 'CLDFRA', 'TKE_PBL']:
            return extract_var_slice(dataset, var_name, time_idx, surface_level, all_levels=True)
        else: 
            return extract_var_slice(dataset, var_name, time_idx, surface_level)

def quantize_data(data: np.ndarray, var_name: str) -> np.ndarray:
    """Quantize data to reduce storage size"""
    metadata = VARIABLE_METADATA.get(var_name, {})
    scale = metadata.get('scale', 1)
    dtype = metadata.get('dtype', 'float32')
    
    # Handle temperature conversion from Kelvin to Celsius
    if metadata.get('convert_temp', False):
        data = data - 273.15
    
    # Handle NaN values
    valid_mask = ~np.isnan(data)
    
    # Scale and convert to integer types for better compression
    if 'int' in dtype or 'uint' in dtype:
        quantized = np.full(data.shape, np.iinfo(np.dtype(dtype)).max, dtype=dtype)
        quantized[valid_mask] = np.clip(data[valid_mask] * scale, 
                                       np.iinfo(np.dtype(dtype)).min, 
                                       np.iinfo(np.dtype(dtype)).max).astype(dtype)
    else: quantized = data.astype(dtype)
        
    return quantized


def get_output_directory(timestamps):
    """Create output directory based on forecast initial date and current date"""
    # Get current date
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Extract initial forecast date from first timestamp
    if timestamps and len(timestamps) > 0:
        try:
            # Parse timestamp format like "2025-05-14_00:00:00"
            initial_timestamp = timestamps[0]
            initial_date = initial_timestamp.split('_')[0].replace('-', '')
        except:
            # Fallback to current date if parsing fails
            initial_date = current_date
    else:
        # Fallback to current date if no timestamps
        initial_date = current_date
    
    # Create directory structure: data/initialdate_currentdate
    base_dir = "data"
    folder_name = f"{initial_date}_{current_date}"
    output_dir = os.path.join(base_dir, folder_name)
    
    # Create base data directory if it doesn't exist
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"Created base directory: {base_dir}")
    
    # Remove existing folder if it exists (overwrite behavior)
    if os.path.exists(output_dir):
        import shutil
        shutil.rmtree(output_dir)
        print(f"Removed existing folder: {output_dir}")
    
    # Create the output directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"Created output directory: {output_dir}")
    
def prepare_data_for_format(data_structure: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare data structure for JSON_GZ format"""
    # JSON needs all NumPy arrays converted to lists (handled by NumpyEncoder)
    return data_structure

def process_compressed_weather_data(dataset, lats: np.ndarray, lons: np.ndarray, 
                                   times: List, timestamps: List) -> Tuple[int, int]:
    """Process all weather data into compressed JSON_GZ format with batched timesteps"""
    
    print(f"\n{'='*60}")
    print(f"PROCESSING WEATHER DATA - COMPRESSED FORMAT (JSON_GZ)")
    print(f"{'='*60}")
    
    # Create output directory with date-based naming
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Extract initial forecast date
    if timestamps and len(timestamps) > 0:
        try:
            initial_date = timestamps[0].split('_')[0].replace('-', '')
        except:
            initial_date = current_date
    
    # Create directory structure
    base_dir = "data"
    folder_name = f"{initial_date}"
    output_dir = os.path.join(base_dir, folder_name)
    
    # Create base data directory if needed
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    
    # Remove existing folder if it exists
    if os.path.exists(output_dir):
        import shutil
        shutil.rmtree(output_dir)
    
    # Create the output directory
    os.makedirs(output_dir)
    print(f"Output directory: {output_dir}")
    
    # Get valid coordinate indices (non-NaN)
    valid_mask = ~(np.isnan(lats) | np.isnan(lons))
    valid_indices = np.where(valid_mask)
    
    valid_lats = lats[valid_indices]
    valid_lons = lons[valid_indices]
    total_points = len(valid_lats)
    
    # Batch configuration
    BATCH_SIZE = 5
    total_batches = (len(times) + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
    
    print(f"Valid grid points: {total_points}")
    print(f"Processing {len(times)} time steps in {total_batches} batches of max {BATCH_SIZE}")
    print(f"Variables: {len(VARIABLES_TO_PROCESS)}")
    
    # Calculate grid dimensions and parameters
    grid_shape = lats.shape  # Should be (156, 147)
    n_lat, n_lon = grid_shape

    # Find grid boundaries and resolution
    lat_min, lat_max = np.nanmin(lats), np.nanmax(lats)
    lon_min, lon_max = np.nanmin(lons), np.nanmax(lons)

    # Calculate resolution (assuming uniform grid)
    lat_resolution = (lat_max - lat_min) / (n_lat - 1) if n_lat > 1 else 0
    lon_resolution = (lon_max - lon_min) / (n_lon - 1) if n_lon > 1 else 0
    
    # Initialize counters
    total_files_created = 0
    total_file_size = 0
    
    # Process timesteps in batches
    for batch_idx in range(total_batches):
        start_time_idx = batch_idx * BATCH_SIZE
        end_time_idx = min(start_time_idx + BATCH_SIZE, len(times))
        batch_times = times[start_time_idx:end_time_idx]
        actual_batch_size = len(batch_times)
        
        print(f"\n--- Processing Batch {batch_idx + 1}/{total_batches} ---")
        print(f"Time steps {start_time_idx + 1}-{end_time_idx} ({actual_batch_size} timesteps)")
        
        # Build the data structure for this batch
        batch_data_structure = {
            'metadata': {
                'batch_info': {
                    'batch_number': batch_idx + 1,
                    'total_batches': total_batches,
                    'batch_size': actual_batch_size
                },
                'initial_timestamp': timestamps[0] if timestamps else f"hour_{times[0]}",
                'final_timestamp': timestamps[-1] if timestamps else f"hour_{times[-1]}",
                'total_timestamps': len(timestamps) if timestamps else len(times),
                'points_per_time': total_points,
                'variable_scales': {
                    var_name: VARIABLE_METADATA.get(var_name, {}).get('scale', 1)
                    for var_name in VARIABLES_TO_PROCESS
                },
            },
            'grid_info': {
                'corner': [float(lat_min), float(lon_min)],  # Bottom-left corner
                'steps': [float(lat_resolution), float(lon_resolution)],  # Lat/lon increments
                'size': [n_lat, n_lon]  # Grid dimensions
            },
            'time_series': []
        }
        
        # Process each time step in this batch
        for local_time_idx, time_value in enumerate(batch_times):
            global_time_idx = start_time_idx + local_time_idx
            print(f"  Processing time step {global_time_idx+1}/{len(times)} (t={time_value:.1f})")
            
            # Load all variables for this timestep
            variables_data = {}
            
            for var_name in VARIABLES_TO_PROCESS:
                try:
                    raw_data = get_variable_data(dataset, var_name, global_time_idx)
                    if raw_data is not None:
                        # Handle regular 2D variables
                        if len(raw_data.shape) == 2:
                            data_1d = raw_data[valid_indices]
                            # Check for valid data before quantization
                            valid_count = np.sum(~np.isnan(data_1d))
                            if valid_count == 0:
                                print(f"    ⚠️ {var_name}: All values are NaN, skipping")
                                continue
                                
                            quantized_data = quantize_data(data_1d, var_name)
                            variables_data[var_name] = quantized_data
                            print(f"    ✅ {var_name}: {len(data_1d)} points ({valid_count} valid)")
                        elif len(raw_data.shape) == 3:
                            # Handle 3D data - use surface level for now
                            surface_data = raw_data[0, :, :]  # First vertical level
                            data_1d = surface_data[valid_indices]
                            valid_count = np.sum(~np.isnan(data_1d))
                            if valid_count == 0:
                                print(f"    ⚠️ {var_name}: All surface values are NaN, skipping")
                                continue
                                
                            quantized_data = quantize_data(data_1d, var_name)
                            variables_data[var_name] = quantized_data
                            print(f"    ✅ {var_name}: {len(data_1d)} points ({valid_count} valid, surface level)")
                        else:
                            print(f"    ⚠️ {var_name}: Unsupported shape {raw_data.shape}")
                    else: print(f"    ❌ {var_name}: No data returned")
                                
                except Exception as e:
                    print(f"    ❌ {var_name}: Error - {e}")
                    continue
            
            # Add timestep to batch data structure
            batch_data_structure['time_series'].append({
                'time': float(time_value),
                'variables': variables_data
            })
        
        # Prepare data for JSON_GZ format
        prepared_data = prepare_data_for_format(batch_data_structure)
        
        # Save compressed batch data
        batch_filename = f"{batch_idx + 1:03d}.{OUTPUT_FORMAT}"
        output_path = os.path.join(output_dir, batch_filename)
        
        try:
            file_size = create_compressed_json(prepared_data, output_path)
                
            total_files_created += 1
            total_file_size += file_size
            
            print(f"  ✅ Batch {batch_idx + 1} saved: {batch_filename}")
            print(f"  ✅ File size: {file_size / (1024*1024):.2f} MB")
            print(f"  ✅ Compression ratio: {(total_points * actual_batch_size * len(VARIABLES_TO_PROCESS) * 4) / file_size:.1f}:1")
            
        except Exception as e:
            print(f"  ❌ Error creating batch {batch_idx + 1} file: {e}")
            continue
    
    # Create a summary file with batch information
    summary_data = {
        'summary': {
            'total_batches': total_batches,
            'batch_size': BATCH_SIZE,
            'total_timesteps': len(times),
            'total_files_created': total_files_created,
            'total_size_mb': total_file_size / (1024*1024),
            'output_format': OUTPUT_FORMAT,
            'variables_processed': VARIABLES_TO_PROCESS,
            'batch_files': [
                f"{i+1:03d}.{OUTPUT_FORMAT}" 
                for i in range(total_files_created)
            ]
        }
    }
    
    summary_path = os.path.join(output_dir, f"batch_summary.json")
    try:
        import json
        with open(summary_path, 'w') as f:
            json.dump(summary_data, f, indent=2)
        print(f"\n✅ Summary file created: {summary_path}")
    except Exception as e:
        print(f"⚠️ Could not create summary file: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ BATCH PROCESSING COMPLETE")
    print(f"✅ Format: {OUTPUT_FORMAT.upper()}")
    print(f"✅ Batches created: {total_files_created}/{total_batches}")
    print(f"✅ Total size: {total_file_size / (1024*1024):.2f} MB")
    print(f"✅ Average batch size: {(total_file_size / total_files_created) / (1024*1024):.2f} MB")
    print(f"✅ Variables included: {len(VARIABLES_TO_PROCESS)}")
    print(f"✅ Total time steps: {len(times)}")
    print(f"{'='*60}")
    
    return total_files_created, total_file_size

def main(file_path):
    """Main function"""
    try:
        dataset = get_dataset(file_path)
        
        # Get basic info
        times, timestamps = get_time_info(dataset)
        lats, lons = extract_var_slice(dataset, 'XLAT', 0), extract_var_slice(dataset, 'XLONG', 0)
        if lats is None or lons is None: 
            print("lat/long data missing!")
            return
        
        print(f"Grid dimensions: {lats.shape}")
        print(f"Lat range: {np.nanmin(lats):.3f} to {np.nanmax(lats):.3f}")
        print(f"Lon range: {np.nanmin(lons):.3f} to {np.nanmax(lons):.3f}")
        print(f"Output format: {OUTPUT_FORMAT.upper()}")
        
        # Process compressed data
        start_time = time.time()
        total_files, total_size = process_compressed_weather_data(dataset, lats, lons, times, timestamps)
        processing_time = time.time() - start_time
        
        # Final report
        print(f"{'='*60}")
        print(f"✅ Format: {OUTPUT_FORMAT.upper()}")
        print(f"✅ Files generated: {total_files}")
        print(f"✅ Total size: {total_size / (1024*1024):.2f} MB")
        print(f"✅ Processing time: {processing_time:.1f} seconds")
        print(f"✅ Variables included: {len(VARIABLES_TO_PROCESS)}")
        print(f"✅ Time steps: {len(times)}")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


class Command(BaseCommand):
    help = 'Process weather data into compressed JSON_GZ format'
    
    def add_arguments(self, parser):
        parser.add_argument(
            'file_path',
            type=str,
            help='Path to the weather dataset file (NetCDF)'
        )

    def handle(self, *args, **options):
        file_path = options['file_path']
        main(file_path)

if __name__ == "__main__": main()