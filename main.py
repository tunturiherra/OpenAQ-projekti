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

