import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import pytz
import os
import csv
import json
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from streamlit_autorefresh import st_autorefresh

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Timezone configuration
GMT_PLUS_7 = pytz.timezone('Asia/Bangkok')

class SolarMonitoringApp:
    def __init__(self):
        # Configuration loading
        self.load_configurations()
        

    def load_configurations(self):
        """Load configuration files"""
        try:
            # Load factory information
            self.factory_info = pd.read_csv("site_location.csv")
            
            # Load secrets (assuming Streamlit secrets management)
            self.BASE_URL = st.secrets["weather"]["base_url"]
            self.TOKEN = st.secrets["weather"]["token"]
        
        except FileNotFoundError as e:
            st.error(f"Configuration file not found: {e}")
            raise
        except KeyError as e:
            st.error(f"Missing configuration key: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, ValueError))
    )
    def fetch_weather_data(self, coordinates, datetime):
        latitude, longtitude = coordinates.split(",")
        url = f"{self.BASE_URL}/{latitude},{longtitude}/{datetime}?unitGroup=metric&key={self.TOKEN}&include=current"
        response = requests.get(url)
        weather_all = response.json().get("days")
        sorted_data_all = {}
        for x in weather_all:
            for key, value in x.items():
                if key == "datetime" or key == "temp" or key == "humidity" or key == "windspeed" or key == "solarradiation" or key == "solarenergy":
                    sorted_data_all.update({key: value})

        weather_current = response.json().get("currentConditions")
        sorted_data_current = {}
        for key, value in weather_current.items():
            if key == "datetime" or key == "temp" or key == "humidity" or key == "windspeed" or key == "solarradiation" or key == "solarenergy":
                sorted_data_current.update({key: value})
        
        return sorted_data_all, sorted_data_current

    def calculate_next_refresh_time(self, current_time):
        """Calculate next refresh time at 60-minute intervals"""
        minutes = (current_time.minute // 60) * 60
        next_refresh = current_time.replace(minute=minutes, second=0, microsecond=0)
        
        if next_refresh == current_time:
            next_refresh += timedelta(minutes=60)
        
        while next_refresh <= current_time:
            next_refresh += timedelta(minutes=60)
        
        return next_refresh + timedelta(minutes=5)

    def auto_refresh_timer(self):
        """Handle auto-refresh logic"""
        current_time = datetime.now(GMT_PLUS_7)
        
        # Refresh only during working hours (8:00 AM to 4:00 PM)
        if 8 <= current_time.hour <= 16:
            next_refresh = self.calculate_next_refresh_time(current_time)
            remaining_seconds = int((next_refresh - current_time).total_seconds())
            
            st_autorefresh(interval=remaining_seconds * 1000, key="precise_auto_refresh")
            st.text(f"Next refresh at: {next_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

    # def process_and_visualize_data(self):
    #     """Process fetched data and create visualizations"""
        

    def run(self):
        """Main application runner"""
        st.set_page_config(page_title="Weather For All Site", layout="centered")
        st.title("Weather For All Site")
        # Apply auto-refresh timer
        self.auto_refresh_timer()

        # Set date range
        hour = datetime.now().strftime("%Y-%m-%dT%H:00:00") # Current hour

        # Fetch data in parallel
        st.write("Fetching weather data for all site today in 1-hour intervals...")
        length = len(self.factory_info)
        locations = self.factory_info["Location"].unique()
        count = 0
        for i in range(length):
            site = self.factory_info.iloc[i]
            site_name = site["Factory"]
            location = site["Location"]
            coordinates = site["Coordinates"]
            if len(locations) > count:
                if locations[count] == location:
                    count += 1
                    st.markdown(f"# :red[{location} Group:]")
            all_day, now = self.fetch_weather_data(coordinates, hour    )
            st.write(f"## {site_name}")
            st.write(f"### All day, Date: {all_day['datetime']}")
            st.write(f"🌡️Temperature: {all_day['temp']}°C,💧Humidity: {all_day['humidity']}%, 💨Wind Speed: {all_day['windspeed']} km/h, ☀️Solar Radiation: {all_day['solarradiation']} W/m², 🔆Solar Energy: {all_day['solarenergy']} MJ/m²")

            st.write(f"### Now: {now['datetime']}")
            st.write(f"🌡️Temperature: {now['temp']}°C, 💧Humidity: {now['humidity']}%, 💨Wind Speed: {now['windspeed']} km/h, ☀️Solar Radiation: {now['solarradiation']} W/m², 🔆Solar Energy: {now['solarenergy']} MJ/m²")

def main():
    try:
        app = SolarMonitoringApp()
        app.run()
    except Exception as e:
        st.error(f"An error occurred: {e}")
        logger.error(f"Unhandled exception: {e}", exc_info=True)

if __name__ == "__main__":
    main()