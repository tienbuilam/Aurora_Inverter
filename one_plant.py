import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import pytz
import os
import csv
import json
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load secrets
API_KEY = st.secrets["aurora"]["api_key"]
USERNAME = st.secrets["aurora"]["username"]
PASSWORD = st.secrets["aurora"]["password"]
BASE_URL = st.secrets["aurora"]["base_url"]

gmt_plus_7 = pytz.timezone('Asia/Bangkok')

# Function to authenticate
def authenticate():
    url = f"{BASE_URL}/authenticate"
    headers = {
        "X-AuroraVision-ApiKey": API_KEY,
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers, auth=(USERNAME, PASSWORD))
    if response.status_code == 200:
        try:
            token = response.json().get("result")
            if token:
                return token
            else:
                print("Token not found in the response.")
        except ValueError:
            print("Failed to parse JSON response.")
    else:
        print(f"Failed to authenticate: {response.status_code} - {response.text}")
    return None

# Function to fetch data for a logger in parallel
def fetch_current_date_parallel(token, entityID, serial, plant_name, start_date, end_date,
                                data_type="GenerationPower", value_type="average", sample_size="Min15"):
    headers = {
        "X-AuroraVision-Token": token,
        "Content-Type": "application/json"
    }
    data_url = (f"{BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/{value_type}"
                f"?sampleSize={sample_size}&startDate={start_date}&endDate={end_date}&timeZone=Asia/Bangkok")
    try:
        response = requests.get(data_url, headers=headers, auth=(USERNAME, PASSWORD))
        if response.status_code == 200:
            data = response.json()
            results = []
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')
                units = entry.get('units', '')
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    results.append([epoch, datetime_str, serial, value, units])
            return serial, results
        else:
            logging.warning(f"Failed to fetch data for {serial} - Status: {response.status_code}")
            return serial, []
    except Exception as e:
        logging.error(f"Error fetching data for {serial}: {e}")
        return serial, []

# Function to fetch all data for a single plant in parallel
def fetch_plant_data_parallel(token, plant_name, loggers, serials, start_date, end_date):
    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                fetch_current_date_parallel, token, logger, serial, plant_name, start_date, end_date
            )
            for logger, serial in zip(loggers, serials)
        ]
        for future in as_completed(futures):
            all_results.append(future.result())
    return all_results

# Streamlit app
st.set_page_config(page_title="One Plant Page", layout="centered")

st.title("Plant Power Output Visualization")

# Authenticate and get token
if "token" not in st.session_state:
    st.session_state.token = authenticate()

token = st.session_state.token

# Load plant names from file
with open('all_inverters.json', 'r') as f:
    inverters = json.load(f)

with open('all_serial.json', 'r') as f:
    logids = json.load(f)

plant_names = list(inverters.keys())

# Dropdown for plant selection
selected_plant = st.selectbox("Select a Plant", plant_names)

if st.button("Fetch and Visualize Data"):
    loggers = inverters.get(selected_plant, [])
    serials = logids.get(selected_plant, [])
    start_date = datetime.now().strftime("%Y%m%d")
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

    # Fetch data for the selected plant in parallel
    plant_data = fetch_plant_data_parallel(token, selected_plant, loggers, serials, start_date, end_date)

    # Process and save data
    df = pd.DataFrame()
    for entityID, results in plant_data:
        if results:
            df_logger = pd.DataFrame(results, columns=["epoch_start", "datetime", "serial", "value", "units"])
            if not df_logger.empty:
                df = pd.concat([df, df_logger], ignore_index=True)

    if not df.empty:
        df['value'] = pd.to_numeric(df['value'], errors='coerce')  # Convert non-numeric to NaN

        filtered_data = df.dropna(subset=['value']).copy()
        filtered_data['datetime'] = pd.to_datetime(filtered_data['datetime'])
        filtered_data = filtered_data.sort_values(by='datetime')

        # Introduce None for breaks in continuity
        time_diff = filtered_data['datetime'].diff().dt.total_seconds()
        threshold = 15 * 60
        filtered_data.loc[time_diff > threshold, 'value'] = None

        with open('all_plants.json', 'r') as f:
            plants = json.load(f)

        entity = None
        for plant, entityID in plants.items():
            if plant == selected_plant:
                entity = entityID

        # Render a clickable title as Markdown in Streamlit
        url = f"https://www.auroravision.net/dashboard/#{entity}"  # Replace with your desired URL
        title_with_link = f"[{selected_plant} AC Output: Power]({url})"
        st.markdown(f"### {title_with_link}")

        # Plot graph
        fig = px.line(
            filtered_data,
            x='datetime',
            y='value',
            color='serial',
            title=f"{selected_plant} Power Output",
            labels={'datetime': 'Time', 'value': 'Power Output (Watts)'},
            template='plotly_white',
        )
        fig.update_yaxes(range=[0, 100000], title="Power Output (Watts)")
        fig.update_traces(mode='lines+markers')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No data available for {selected_plant}.")