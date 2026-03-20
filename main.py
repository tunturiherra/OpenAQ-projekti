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

# datan tallennus tietokantaan
def save_to_db(df, location, city_name):
    if df is None or df.empty:
        return 0

    count = 0

    with get_conn() as conn:
        with conn.cursor() as cur:

            # Maa
            country_code = location.get("country", {}).get("code", "XX")
            country_name = location.get("country", {}).get("name", "Unknown")
            cur.execute("SELECT id FROM countries WHERE code = %s", (country_code,))
            row = cur.fetchone()
            if row:
                country_id = row[0]
            else:
                cur.execute("INSERT INTO countries (code, name) VALUES (%s, %s) RETURNING id",
                            (country_code, country_name))
                country_id = cur.fetchone()[0]

            # Kaupunki
            cur.execute("SELECT id FROM cities WHERE country_id = %s AND name = %s", (country_id, city_name))
            row = cur.fetchone()
            if row:
                city_id = row[0]
            else:
                cur.execute("INSERT INTO cities (country_id, name) VALUES (%s, %s) RETURNING id",
                            (country_id, city_name))
                city_id = cur.fetchone()[0]

            # Mittauspiste
            cur.execute("SELECT id FROM locations WHERE openaq_id = %s", (location["id"],))
            row = cur.fetchone()
            if row:
                location_id = row[0]
            else:
                cur.execute("INSERT INTO locations (city_id, openaq_id, name) VALUES (%s, %s, %s) RETURNING id",
                            (city_id, location["id"], location["name"]))
                location_id = cur.fetchone()[0]

            conn.commit()

            # Käydään CSV:n rivit läpi
            for _, row in df.iterrows():
                try:
                    # Parametrisoidut kyselyt SQL-injektion torjumiseksi
                    cur.execute("SELECT id FROM parameters WHERE name = %s", (row["parameter"],))
                    result = cur.fetchone()
                    if result:
                        parameter_id = result[0]
                    else:
                        cur.execute("INSERT INTO parameters (name, unit) VALUES (%s, %s) RETURNING id",
                                    (row["parameter"], row["units"]))
                        parameter_id = cur.fetchone()[0]

                    # Sensori
                    cur.execute("SELECT id FROM sensors WHERE openaq_sensor_id = %s", (int(row["sensors_id"]),))
                    result = cur.fetchone()
                    if result:
                        sensor_id = result[0]
                    else:
                        cur.execute("INSERT INTO sensors (location_id, parameter_id, openaq_sensor_id) VALUES (%s, %s, %s) RETURNING id",
                                    (location_id, parameter_id, int(row["sensors_id"])))
                        sensor_id = cur.fetchone()[0]

                    # Mittaus
                    cur.execute("SELECT id FROM measurements WHERE sensor_id = %s AND measured_at = %s",
                                (sensor_id, row["datetime"]))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO measurements (sensor_id, measured_at, value) VALUES (%s, %s, %s)",
                                    (sensor_id, row["datetime"], row["value"]))
                        count += 1

                except Exception as e:
                    print(f"Virhe rivillä: {e}")
                    conn.rollback()
                    continue

            conn.commit()

    return count
