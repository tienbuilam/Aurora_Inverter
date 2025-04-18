import streamlit as st
import pandas as pd
import requests
import pytz
import os
import csv
import json
import logging
from datetime import datetime, timedelta
from streamlit_date_picker import date_range_picker, PickerType
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import io

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Timezone configuration
GMT_PLUS_7 = pytz.timezone('Asia/Bangkok')

# Page config
st.set_page_config(page_title="Energy Computing Page", layout="wide")


class SolarMonitoringApp:
    def __init__(self):
        # Configuration loading
        self.load_configurations()

        # Authentication
        self.token = None

    def load_configurations(self):
        """Load configuration files"""
        try:
            # Load inverters and serials from JSON files
            self.all_plants = pd.read_excel("All sites in plant.xlsx")

            # Load secrets
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
            response = requests.get(
                url, headers=headers, auth=(self.USERNAME, self.PASSWORD))
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
    def fetch_1_day_energy_data(self, token, plant_name, start, end):
        """Fetch 1 day energy data for a specific plant"""
        headers = {
            "X-AuroraVision-Token": token,
            "Content-Type": "application/json"
        }

        entityID = self.all_plants[self.all_plants['All Sites']
                                   == plant_name]['All Plants'].values[0]
        if pd.isna(entityID):
            return None

        elif isinstance(entityID, str):
            entityID = entityID.split(', ')
            all_data = pd.DataFrame()

            for id in entityID:
                url = f"{self.BASE_URL}/v1/stats/energy/timeseries/{id}/GenerationEnergy/delta?sampleSize=Day&startDate={start}&endDate={end}&timeZone=Asia/Bangkok"
                response = requests.get(url, headers=headers)
                data = response.json()
                results = data.get('result')
                for result in results:
                    ts = result['start']
                    result.pop('units')
                    # Explicitly convert timestamp to datetime in GMT+7 timezone
                    dt = datetime.fromtimestamp(ts, GMT_PLUS_7)
                    result['start'] = dt.strftime("%Y-%m-%d")
                    all_data = pd.concat(
                        [all_data, pd.DataFrame([result])], ignore_index=True)

            # Ensure consistent data types before grouping
            all_data['value'] = pd.to_numeric(
                all_data['value'], errors='coerce')
            result = all_data.groupby('start')['value'].sum().reset_index()
            return result

        elif isinstance(entityID, (int, float)) and not pd.isna(entityID):
            entityID = str(int(entityID))
            all_data = pd.DataFrame()
            url = f"{self.BASE_URL}/v1/stats/energy/timeseries/{entityID}/GenerationEnergy/delta?sampleSize=Day&startDate={start}&endDate={end}&timeZone=Asia/Bangkok"
            response = requests.get(url, headers=headers)
            data = response.json()
            results = data.get('result')
            for result in results:
                ts = result['start']
                result.pop('units')
                # Explicitly convert timestamp to datetime in GMT+7 timezone
                dt = datetime.fromtimestamp(ts, GMT_PLUS_7)
                result['start'] = dt.strftime("%Y-%m-%d")
                all_data = pd.concat(
                    [all_data, pd.DataFrame([result])], ignore_index=True)

            # Ensure consistent data types
            all_data['value'] = pd.to_numeric(
                all_data['value'], errors='coerce')
            return all_data

        return None

    def process_and_visualize_data(self):
        """Process fetched data and create visualizations"""
        # First authenticate
        token = self.authenticate()
        if not token:
            return

        # Get current time in GMT+7
        current_time = datetime.now(GMT_PLUS_7)
        # Display current time in GMT+7 for debugging
        st.sidebar.write(
            f"Current time (GMT+7): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        start, end = current_time, current_time + timedelta(days=1)

        # Date range picker
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 📅Date Range Picker")
            default_start, default_end = start, end
            date_range_string = date_range_picker(picker_type=PickerType.date,
                                                  start=default_start, end=default_end,
                                                  key='date_range_picker')
        with col2:
            st.write("#### ⚠️Note:")
            st.write(
                "The data will return from start date to end date - 1 day")
            st.write(
                f"If you pick the end date > {default_end.strftime('%Y-%m-%d')}, it will be set to the {default_end.strftime('%Y-%m-%d')}")
            st.write(
                f"If you pick the start date >= {default_end.strftime('%Y-%m-%d')}, it will be set to the {default_start.strftime('%Y-%m-%d')}")
            st.write(
                "Sometimes the data will be wrong (Aurora Vision error), please check the data manually.")

        if date_range_string:
            start_str, end_str = date_range_string
            # Convert string dates to datetime objects in GMT+7
            start = datetime.strptime(
                start_str, '%Y-%m-%d').replace(tzinfo=GMT_PLUS_7)
            end = datetime.strptime(
                end_str, '%Y-%m-%d').replace(tzinfo=GMT_PLUS_7)

            if end > default_end:
                end = default_end
            if start >= default_end:
                start = default_start

        # Format dates for API call (ensure we're using the correct timezone)
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")

        st.sidebar.write(f"API Date Range: {start_date} to {end_date}")

        st.markdown("### ☀️Energy Generation Data")

        # IMPORTANT: Clean up previous data files before fetching new data
        if os.path.exists("energy_data"):
            # Clear all previous data files
            for file in os.listdir("energy_data"):
                file_path = os.path.join("energy_data", file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        logger.info(f"Deleted previous data file: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting file {file_path}: {e}")
        else:
            # Create directory if it doesn't exist
            os.makedirs("energy_data", exist_ok=True)

        # Fetch data for all plants
        all_plants = self.all_plants['All Sites'].unique()
        for plant in all_plants:
            data = self.fetch_1_day_energy_data(
                token, plant, start_date, end_date)
            if data is not None:
                # Save data to CSV
                filename = f"energy_data/{plant}.csv"
                data.to_csv(filename, index=False)

        # Initialize empty DataFrame
        all_plants_data = pd.DataFrame()
        excel_sites = self.all_plants['All Sites'].tolist()

        # Read and combine data from all plant files
        energy_files = os.listdir(
            "energy_data") if os.path.exists("energy_data") else []
        for site in excel_sites:
            file = f"{site}.csv"
            if file in energy_files:
                plant_data = pd.read_csv(f"energy_data/{file}")
                plant_data['Plant'] = site
                all_plants_data = pd.concat(
                    [all_plants_data, plant_data], ignore_index=True)
            else:
                # Create empty data for sites without files
                date_range = pd.date_range(
                    start=start.date(), end=(end-timedelta(days=1)).date())
                empty_data = pd.DataFrame({
                    'start': [d.strftime('%Y-%m-%d') for d in date_range],
                    'value': '',
                    'Plant': site
                })
                all_plants_data = pd.concat(
                    [all_plants_data, empty_data], ignore_index=True)

        if not all_plants_data.empty:
            # Pivot the data with dates as rows and plants as columns
            pivot_table = all_plants_data.pivot(
                index='start',
                columns='Plant',
                values='value'
            )

            # Convert empty strings and non-numeric values to NaN first
            pivot_table = pivot_table.apply(pd.to_numeric, errors='coerce')

            # Round the numeric values
            pivot_table = pivot_table.round(0)

            # Reorder columns to match Excel order
            if all(site in pivot_table.columns for site in excel_sites):
                pivot_table = pivot_table[excel_sites]

            # Sort index (dates) in ascending order
            pivot_table = pivot_table.sort_index()

            # Replace NaN with empty string for display
            pivot_table_display = pivot_table.fillna('')

            # Style the table
            styled_table = pivot_table_display.style\
                .format(lambda x: '{:.0f}'.format(float(x)) if isinstance(x, (int, float)) and not pd.isna(x) else '')\
                .set_properties(**{'text-align': 'center'})\
                .set_table_styles([
                    {'selector': 'th', 'props': [('text-align', 'center')]},
                    {'selector': '', 'props': [('border', '1px solid grey')]}
                ])

            # Display the table
            st.dataframe(styled_table, use_container_width=True)

            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                pivot_table.to_excel(writer, sheet_name='Energy Data')
                # Auto-adjust columns' width
                worksheet = writer.sheets['Energy Data']
                for idx, col in enumerate(pivot_table.columns):
                    series = pivot_table[col]
                    max_length = max(
                        series.astype(str).apply(len).max(),
                        len(str(col))
                    ) + 1
                    worksheet.set_column(idx + 1, idx + 1, max_length)
            excel_buffer.seek(0)

            # Create download button
            st.download_button(
                label="Download Excel file",
                data=excel_buffer,
                file_name="energy_generation_data.xlsx",
                mime="application/vnd.ms-excel",
                on_click=lambda: setattr(
                    st.session_state, 'download_clicked', True)
            )

    def run(self):
        """Main application logic"""
        st.title("Energy Computing for All Plants")

        # Process and visualize data
        self.process_and_visualize_data()


def main():
    app = SolarMonitoringApp()
    app.run()


if __name__ == "__main__":
    main()
