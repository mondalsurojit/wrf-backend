# backend/forecast_runner.py
import os
import datetime
from my_models.graphcast import run_graphcast
from my_models.pangu import run_pangu
from my_models.fourcastnet import run_fourcastnet
from my_models.aurora import run_aurora
from my_models.graphcast1p import run_graphcast1p
from my_utils import save_forecast_as_json

CITY_COORDS = {"lat": 17.385, "lon": 78.486}
OUTPUT_DIR = "/app/data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

models = {
    "graphcast": run_graphcast,
    "panguweather": run_pangu,
    "fourcastnetv2-small": run_fourcastnet,
    "aurora-2.5-finetuned": run_aurora,
    "graphcast-1p00": run_graphcast1p,
}

today = datetime.datetime.utcnow().strftime("%Y%m%d")

for model_name, model_fn in models.items():
    print(f"Running {model_name}...")
    forecast = model_fn(CITY_COORDS)
    output_path = os.path.join(OUTPUT_DIR, f"{model_name}_{today}.json")
    save_forecast_as_json(forecast, model_name, output_path)
    print(f"Saved: {output_path}")
