import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta
import numpy as np
import requests


filter = '1223'
filterCopy = filter
region = 'DE-LU'
regionCopy = region
resolution = 'hour'
timestamp = "1627855200000"

url = "https://www.smard.de/app/chart_data/{filter}/{region}/{filterCopy}_{regionCopy}_{resolution}_{timestamp}.json"


response = requests.get(url)
response.raise_for_status()
data = response.json()