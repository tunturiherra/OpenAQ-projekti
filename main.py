import io
import gzip
import requests
import pandas as pd
import psycopg2
import os
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

# Yhteys Postgresiin
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PWD"),
    )

# määritetään kaupungin rajat koordinaatteina. Tulos annetaan OPENAQ:lle parametrina
def get_bbox(city):
    url = f"https://nominatim.openstreetmap.org/search?q={quote(city)}&format=json"
    response = requests.get(url, headers={"User-Agent": "OpenAQImporter"}).json()

    if not response:
        print(f"Kaupunkia '{city}' ei löydy.")
        return None

    min_lat, max_lat, min_lon, max_lon = response[0]["boundingbox"]
    return f"{min_lon},{min_lat},{max_lon},{max_lat}"

# Palauttaa listauksen mittauspisteistä
def get_locations_by_bbox(bbox):
    response = requests.get(
        "https://api.openaq.org/v3/locations",
        params={"limit": 1000, "page": 1, "order_by": "id", "sort_order": "asc", "bbox": bbox},
        headers={"X-API-Key": os.getenv("OPENAQ_API_KEY")},
    )
    if response.status_code == 200:
        return response.json()["results"]
    print(f"Virhe locations-haussa: {response.status_code}")
    return []

S3_BASE_URL = "https://openaq-data-archive.s3.amazonaws.com"

# hakee csv.tiedoston s3:sta päivämäärän ja sijainti id:n perusteella
def fetch_s3_day(location_id, year, month, day):
    filename = f"location-{location_id}-{year}{month:02d}{day:02d}.csv.gz"
    path = (
        f"records/csv.gz/locationid={location_id}"
        f"/year={year}/month={month:02d}"
        f"/{filename}"
    )
    url = f"{S3_BASE_URL}/{path}"
    response = requests.get(url, timeout=30)

    if response.status_code == 404:
        return None
    response.raise_for_status()

    with gzip.open(io.BytesIO(response.content)) as f:
        return pd.read_csv(f)