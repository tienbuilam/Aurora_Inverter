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
        
        # Authentication
        self.token = None

    def check_inverter_time(self, data, plant_name):
        """Check if inverter data is outdated"""
        data['datetime'] = pd.to_datetime(data['datetime'])
        time = data[data['value'].notnull()]['datetime'].iloc[-1]
        datetime_obj = datetime.now(GMT_PLUS_7)

        # Ensure both have the same timezone (GMT+7)
        datetime_obj = datetime_obj.astimezone(pytz.timezone('Asia/Bangkok'))
        timestamp_obj = time.tz_localize('Asia/Bangkok')

        serial_id = data['serial'].iloc[0]
        
        if datetime_obj - timedelta(minutes=30) > timestamp_obj:
            timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M')
            msg = f"**{plant_name}**, inverter **{serial_id}** outdated.\nLast update: {timestamp_str}"
            st.warning(msg, icon="⚠️")
            return False
        else:
            return True

    def compare_latest_inverter_power(self, data, plant_name):
        """Compare power output of inverters"""
        time = data[data['value'].notnull()]['datetime'].iloc[-1]
        data = data[data['datetime'] == time].sort_values(by='value', ascending=False)
        serial_ids = data['serial'].unique()
        
        if data['value'].iloc[0] > 50:
            for i in range(1, len(serial_ids)):
                underperforming_serial = serial_ids[i]
                
                if data['value'].iloc[i] < data['value'].iloc[0] * 0.25:
                    current_value = round(data['value'].iloc[i], 2)
                    time_str = time.strftime('%Y-%m-%d %H:%M')
                    msg = f"**{plant_name}**, inverter **{underperforming_serial}** is underperforming with {current_value} kW.\nTime: {time_str}"
                    st.warning(msg, icon="⚠️")
        else:
            return None

    def check_low_power_period(self, data, plant_name):
        """Check for low power output and high power drop"""
        serial_id = data['serial'].iloc[0]
        time = data[data['value'].notnull()]['datetime']
        value = data[data['value'].notnull()]['value']
        
        if value.iloc[-1] < 5000 and value.size > 3:
            if value.iloc[-2] < 5000 and value.iloc[-3] < 5000:
                start_time = time.iloc[-3].strftime('%Y-%m-%d %H:%M')
                end_time = time.iloc[-1].strftime('%Y-%m-%d %H:%M')
                msg = f"**{plant_name}**, inverter **{serial_id}** detects low power.\nFrom {start_time} to {end_time}"
                st.warning(msg, icon="⚠️")
            elif value.iloc[-2] > 50000:
                start_time = time.iloc[-2].strftime('%Y-%m-%d %H:%M')
                end_time = time.iloc[-1].strftime('%Y-%m-%d %H:%M')         
                msg = f"**{plant_name}**, inverter **{serial_id}** detects high power drop.\nFrom {start_time} to {end_time}"
                st.warning(msg, icon="⚠️")

    def load_configurations(self):
        """Load configuration files"""
        try:
            # Load inverters and serials from JSON files
            with open('all_inverters.json', 'r') as f:
                self.inverters = json.load(f)
            
            with open('all_serial.json', 'r') as f:
                self.serials = json.load(f)
            
            with open('all_plants.json', 'r') as f:
                self.plants = json.load(f)
            
            # Load secrets (assuming Streamlit secrets management)
            self.BOT_TOKEN = st.secrets["telegram"]["bot_token"]
            self.CHAT_ID = st.secrets["telegram"]["chat_id"]
            self.API_KEY = st.secrets["aurora"]["api_key"]
            self.USERNAME = st.secrets["aurora"]["username"]
            self.PASSWORD = st.secrets["aurora"]["password"]
            self.BASE_URL = st.secrets["aurora"]["base_url"]
        
        except FileNotFoundError as e:
            st.error(f"Configuration file not found: {e}")
            raise
        except KeyError as e:
            st.error(f"Missing configuration key: {e}")
            raise

    def authenticate(self):
        """Authenticate and get token"""
        url = f"{self.BASE_URL}/authenticate"
        headers = {
            "X-AuroraVision-ApiKey": self.API_KEY,
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, auth=(self.USERNAME, self.PASSWORD))
            response.raise_for_status()
            self.token = response.json().get("result")
            
            if not self.token:
                st.error("Failed to retrieve authentication token.")
                return None
            
            return self.token
        
        except requests.RequestException as e:
            st.error(f"Authentication failed: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, ValueError))
    )
    def fetch_data_for_inverter(self, token, entityID, serial, plant_name, start_date, end_date):
        """Fetch data for a single inverter"""
        headers = {
            "X-AuroraVision-Token": token,
            "Content-Type": "application/json"
        }

        data_url = (f"{self.BASE_URL}/v1/stats/power/timeseries/{entityID}/GenerationPower/average"
                    f"?sampleSize=Min15&startDate={start_date}&endDate={end_date}&timeZone=Asia/Bangkok")

        try:
            response = requests.get(data_url, headers=headers, auth=(self.USERNAME, self.PASSWORD))
            response.raise_for_status()
            
            data = response.json()
            results = []
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')
                units = entry.get('units', '')

                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(GMT_PLUS_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    results.append([epoch, datetime_str, serial, value, units])
            
            return plant_name, serial, results
        
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {serial}: {e}")
            return plant_name, serial, []

    def fetch_all_data_parallel(self, token, start_date, end_date):
        """Fetch data for all inverters in parallel"""
        all_results = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            for plant_name in self.inverters:
                plant_inverters = self.inverters.get(plant_name, [])
                plant_serials = self.serials.get(plant_name, [])

                futures.extend([
                    executor.submit(
                        self.fetch_data_for_inverter,
                        token,
                        inverter_id,
                        serial,
                        plant_name,
                        start_date,
                        end_date
                    )
                    for inverter_id, serial in zip(plant_inverters, plant_serials)
                ])

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        all_results.append(result)
                except Exception as e:
                    logger.error(f"Error processing future: {str(e)}")

        return all_results

    def save_inverter_data(self, all_data):
        """Save fetched inverter data to CSV files"""
        for plant_name, serial, results in all_data:
            if results:
                folder_path = f"temp/{plant_name}"
                os.makedirs(folder_path, exist_ok=True)
                filename = os.path.join(folder_path, f"{serial}.csv")
                
                with open(filename, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(["epoch_start", "datetime", "serial", "value", "units"])
                    writer.writerows(results)

    def process_and_visualize_data(self):
        """Process fetched data and create visualizations"""
        for plant_name, serials in self.serials.items():
            df = pd.DataFrame()
            drop = []  # List of deactivated inverters

            for serial in serials:
                filename = f"temp/{plant_name}/{serial}.csv"
                if os.path.exists(filename):
                    df_logger = pd.read_csv(filename)
                    
                    if df_logger['value'].notnull().any():
                        if self.check_inverter_time(df_logger, plant_name):
                            self.check_low_power_period(df_logger, plant_name)
                        df = pd.concat([df, df_logger], ignore_index=True)
                    else:
                        drop.append([plant_name, serial])

            if not df.empty:
                # Add warning for deactivated inverters
                for plant_name, serial in drop:
                    st.warning(f"**{plant_name}**, inverter **{serial}** is deactivated.", icon="⚠️")

                # Process and visualize data
                filtered_data = df.dropna(subset=['value']).copy()
                filtered_data['datetime'] = pd.to_datetime(filtered_data['datetime'])
                filtered_data = filtered_data.sort_values(by='datetime')

                # Handle data continuity
                time_diff = filtered_data['datetime'].diff().dt.total_seconds()
                threshold = 15 * 60
                filtered_data.loc[time_diff > threshold, 'value'] = None
                filtered_data['value'] = filtered_data['value'] / 1000  # Convert to kW
                
                self.compare_latest_inverter_power(filtered_data, plant_name)

                # Get entity for plant
                entity = self.plants.get(plant_name)

                # Render clickable title
                url = f"https://www.auroravision.net/dashboard/#{entity}"
                title_with_link = f"[{plant_name} AC Output: Power]({url})"
                st.markdown(f"### {title_with_link}")

                # Plot graph
                self.plot_power_output(filtered_data, plant_name)

    def plot_power_output(self, filtered_data, plant_name):
        """Create power output plot"""
        fig = px.line(
            filtered_data,
            x='datetime',
            y='value',
            color='serial',
            title=f"{plant_name} Power Output",
            labels={'datetime': 'Time', 'value': 'Power Output (kW)'},
            template='plotly_white'
        )

        # Set x-axis range
        current_date = datetime.now(GMT_PLUS_7).date()
        start_time = GMT_PLUS_7.localize(datetime.combine(current_date, datetime.strptime("06:00", "%H:%M").time()))
        end_time = GMT_PLUS_7.localize(datetime.combine(current_date, datetime.strptime("18:00", "%H:%M").time()))

        fig.update_xaxes(
            range=[start_time, end_time],
            tickformat="%H:%M",
            dtick=3600000*2,  # Show tick every 2 hours
            title="Time (Hours)"
        )

        fig.update_yaxes(range=[0, 100], title="Power Output (kW)")
        fig.update_traces(hovertemplate='%{x} <br> Power: %{y:.2f} kW', mode='lines+markers')

        st.plotly_chart(fig, use_container_width=True)

    def calculate_next_refresh_time(self, current_time):
        """Calculate next refresh time at 15-minute intervals"""
        minutes = (current_time.minute // 15) * 15
        next_refresh = current_time.replace(minute=minutes, second=0, microsecond=0)
        
        if next_refresh == current_time:
            next_refresh += timedelta(minutes=15)
        
        while next_refresh <= current_time:
            next_refresh += timedelta(minutes=15)
        
        return next_refresh + timedelta(minutes=2)

    def auto_refresh_timer(self):
        """Handle auto-refresh logic"""
        current_time = datetime.now(GMT_PLUS_7)
        
        # Refresh only during working hours (8:00 AM to 4:00 PM)
        if 8 <= current_time.hour <= 16:
            next_refresh = self.calculate_next_refresh_time(current_time)
            remaining_seconds = int((next_refresh - current_time).total_seconds())
            
            st_autorefresh(interval=remaining_seconds * 1000, key="precise_auto_refresh")
            st.text(f"Next refresh at: {next_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

    def run(self):
        """Main application runner"""
        st.set_page_config(page_title="Solar Plant Monitoring", layout="centered")
        st.title("Solar Plant Power Output Visualization")

        self.authenticate()

        # Apply auto-refresh timer
        self.auto_refresh_timer()

        # Set date range
        start_date = datetime.now().strftime("%Y%m%d")
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

        # Fetch data in parallel
        st.write("Fetching data for all plants today in 15-minute intervals...")
        all_data = self.fetch_all_data_parallel(self.token, start_date, end_date)

        # Save inverter data
        self.save_inverter_data(all_data)

        st.success("Data fetching completed. Generating graphs...")

        # Process and visualize data
        self.process_and_visualize_data()

def main():
    try:
        app = SolarMonitoringApp()
        app.run()
    except Exception as e:
        st.error(f"An error occurred: {e}")
        logger.error(f"Unhandled exception: {e}", exc_info=True)

if __name__ == "__main__":
    main()
