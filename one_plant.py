import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import pytz
import os
import csv
import json
from datetime import datetime, timedelta
import time

# Load secrets
API_KEY = st.secrets["aurora"]["api_key"]
USERNAME = st.secrets["aurora"]["username"]
PASSWORD = st.secrets["aurora"]["password"]
BASE_URL = st.secrets["aurora"]["base_url"]

def authenticate():
    print("Authenticating...")
    url = f"{BASE_URL}/authenticate"

    # Add the API key to the headers
    headers = {
        "X-AuroraVision-ApiKey": API_KEY,
        "Content-Type": "application/json"
    }

    # Use Basic Authentication with the username and password
    response = requests.get(url, headers=headers, auth=(USERNAME, PASSWORD))

    if response.status_code == 200:
        try:
            # Extract the token from the JSON response body
            token = response.json().get("result")
            if token:
                print(f"Authentication successful! Token: {token}")
                return token
            else:
                print("Token not found in the response.")
                return None
        except ValueError:
            print("Failed to parse JSON response.")
            return None
    else:
        print(f"Failed to authenticate: {response.status_code} - {response.text}")
        return None

gmt_plus_7 = pytz.timezone('Asia/Bangkok')

# Function to fetch data
@st.cache_data
def fetch_current_date(token, entityID, plant_name, start_date, end_date,
                       data_type="GenerationPower", value_type="average", sample_size="Min15"):
    headers = {
        "X-AuroraVision-Token": token,
        "Content-Type": "application/json"
    }

    folder_path = f"temp/{plant_name}"
    os.makedirs(folder_path, exist_ok=True)
    filename = os.path.join(folder_path, f"{entityID}.csv")

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["epoch_start", "datetime", "entityID", "value", "units"])

    data_url = (f"{BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/{value_type}"
                f"?sampleSize={sample_size}&startDate={start_date}&endDate={end_date}&timeZone=Asia/Bangkok")
    response = requests.get(data_url, headers=headers, auth=(USERNAME, PASSWORD))

    if response.status_code == 200:
        data = response.json()
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')
                units = entry.get('units', '')

                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    writer.writerow([epoch, datetime_str, entityID, value, units])
    else:
        st.error(f"Failed to fetch data: {response.status_code} - {response.text}")

# Streamlit App
st.title("Plant Power Output Visualization")

# Get token for API authentication
if "token" not in st.session_state:
    st.session_state.token = authenticate()

token = st.session_state.token

# Load plant names from the JSON file
with open('all_inverter.json', 'r') as f:
    inverters = json.load(f)

plant_names = list(inverters.keys())

# Dropdown for plant selection
selected_plant = st.selectbox("Select a Plant", plant_names)

if st.button("Fetch and Visualize Data"):
    loggers = inverters.get(selected_plant, [])
    start_date = datetime.now().strftime("%Y%m%d")
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

    for logger in loggers:
        fetch_current_date(token, logger, selected_plant, start_date, end_date)

    # Read and combine data from CSV files for the selected plant
    df = pd.DataFrame()
    for logger in loggers:
        filename = f"temp/{selected_plant}/{logger}.csv"
        if os.path.exists(filename):
            df_logger = pd.read_csv(filename)
            df = pd.concat([df, df_logger], ignore_index=True)
    
    # Filter and visualize data
    if not df.empty:
        filtered_data = df.dropna(subset=['value'])
        filtered_data['datetime'] = pd.to_datetime(filtered_data['datetime'])

        # Sort by datetime to ensure proper ordering
        filtered_data = filtered_data.sort_values(by='datetime')

        # Introduce None for breaks in continuity
        time_diff = filtered_data['datetime'].diff().dt.total_seconds()
        threshold = 15 * 60  # 15 minutes in seconds
        filtered_data.loc[time_diff > threshold, 'value'] = None

        with open('all_plant.json', 'r') as f:
            plants = json.load(f)

        entity = None
        for plant, entityID in list(plants.items()):
            if plant == selected_plant:
                entity = entityID

        # Render a clickable title as Markdown in Streamlit
        url = f"https://www.auroravision.net/dashboard/#{entity}"  # Replace with your desired URL
        title_with_link = f"[{selected_plant} AC Output: Power]({url})"
        st.markdown(f"### {title_with_link}")

        # Plot using Plotly
        fig = px.line(
            filtered_data,
            x='datetime',
            y='value',
            color='entityID',
            # title=f'{selected_plant} AC Output: Power',
            labels={'datetime': 'Time', 'value': 'Power Output (Watts)'},
            template='plotly_white'
        )
        # Update y-axis with a fixed maximum value of 100kW (100,000 Watts)
        fig.update_yaxes(range=[0, 100000], title="Power Output (Watts)")
        fig.update_traces(mode='lines+markers')
        st.plotly_chart(fig)
    else:
        st.warning("No data available for the selected plant.")
