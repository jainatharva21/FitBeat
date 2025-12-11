import os
import ast
import time
import requests
import pandas as pd
from datetime import datetime

VISUAL_CROSSING_API_KEY = '7HUYR79HJ7UDJLUFHMCLPQSX9'


def parse_latlng(latlng_str):
    if pd.isna(latlng_str):
        return None, None
    try:
        # safely evaluate the string as a Python list
        coords = ast.literal_eval(latlng_str)
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            return float(coords[0]), float(coords[1])
    except Exception:
        pass
    return None, None


def fetch_weather_for_run(lat, lon, run_dt):
    # If no valid coordinates, skip
    if lat is None or lon is None:
        return {
            "temp": None,
            "humidity": None,
            "windspeed": None,
            "precip": None,
            "conditions": None,
        }

    # Visual Crossing timeline API endpoint
    # We'll query for the day of the run and then match the hour.
    date_str = run_dt.strftime("%Y-%m-%d")
    location = f"{lat},{lon}"

    url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{location}/{date_str}"
    )

    params = {
        "key": VISUAL_CROSSING_API_KEY,
        "unitGroup": "metric",
        "include": "hours",
        "contentType": "json",
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Weather API error for {location} on {date_str}: {e}")
        return {
            "temp": None,
            "humidity": None,
            "windspeed": None,
            "precip": None,
            "conditions": None,
        }

    days = data.get("days", [])
    if not days:
        return {
            "temp": None,
            "humidity": None,
            "windspeed": None,
            "precip": None,
            "conditions": None,
        }

    hours = days[0].get("hours", [])
    if not hours:
        return {
            "temp": None,
            "humidity": None,
            "windspeed": None,
            "precip": None,
            "conditions": None,
        }

    # Find the hour closest to run_dt
    run_minutes = run_dt.hour * 60 + run_dt.minute
    best_hour = None
    best_diff = float("inf")

    for h in hours:
        t_str = h.get("datetime", "00:00:00")
        try:
            hh, mm, *_ = map(int, t_str.split(":"))
        except Exception:
            continue
        minutes = hh * 60 + mm
        diff = abs(minutes - run_minutes)
        if diff < best_diff:
            best_diff = diff
            best_hour = h

    if best_hour is None:
        return {
            "temp": None,
            "humidity": None,
            "windspeed": None,
            "precip": None,
            "conditions": None,
        }

    return {
        "temp": best_hour.get("temp"),
        "humidity": best_hour.get("humidity"),
        "windspeed": best_hour.get("windspeed"),
        "precip": best_hour.get("precip"),
        "conditions": best_hour.get("conditions"),
    }

def main():
    input_path = "data/music_running_dataset.csv"
    df = pd.read_csv(input_path)
    print("Loaded dataset:", df.shape)

    # Ensuring start_dt is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df["start_dt"]):
        df["start_dt"] = pd.to_datetime(df["start_dt"])

    weather_rows = []
    for _, row in df.iterrows():
        lat, lon = parse_latlng(row.get("start_latlng"))
        run_dt = row["start_dt"]
        weather = fetch_weather_for_run(lat, lon, run_dt)
        weather_rows.append(weather)
        time.sleep(0.5)

    # Convert weather_rows to DataFrame and concat
    weather_df = pd.DataFrame(weather_rows)
    final_df = pd.concat([df.reset_index(drop=True), weather_df], axis=1)
    output_path = "analysis_dataset/music_running_weather.csv"
    final_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
