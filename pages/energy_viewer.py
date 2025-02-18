import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from functools import wraps

# Load secrets
API_KEY = st.secrets["aurora"]["api_key"]
USERNAME = st.secrets["aurora"]["username"]
PASSWORD = st.secrets["aurora"]["password"]
BASE_URL = st.secrets["aurora"]["base_url"]

gmt_plus_7 = pytz.timezone('Asia/Bangkok')

# Function to authenticate
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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def fetch_inverter_power(token, entityID, plant_name, start_date, end_date, 
                        data_type="GenerationPower", value_type="average", sample_size="Min15"):
    # Prepare headers for API requests
    headers = {
        "X-AuroraVision-Token": token,
        "Content-Type": "application/json"
    }

    # Get plant name for the CSV file
    folder_path = f"temp/{plant_name}"
    os.makedirs(folder_path, exist_ok=True)  # Ensure the folder is created

    # Prepare the filename with the correct path
    filename = os.path.join(folder_path, f"{plant_name}_power.csv")
    
    # Clear the file before fetching new data
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["epoch_start", "datetime", "value", "units"])  # Header row

    data_url = (f"{BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/{value_type}"
                f"?sampleSize={sample_size}&startDate={start_date}&endDate={end_date}&timeZone=Asia/Bangkok")

    # Make the API request
    response = requests.get(data_url, headers=headers, auth=(USERNAME, PASSWORD))

    if response.status_code == 200:
        data = response.json()

        # Open the CSV file in append mode and write the data
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')  # Handle missing values gracefully
                units = entry.get('units', '')

                # Convert epoch to readable datetime in GMT+7
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    gmt_plus_7 = pytz.timezone('Asia/Bangkok')
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')

                    # Write each entry to the CSV
                    writer.writerow([epoch, datetime_str, value, units])
    else:
        print(f"Failed to fetch data for {start_date} to {end_date}: {response.status_code} - {plant_name}")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def fetch_grid_power_export(token, entityID, plant_name, start_date, end_date,
                            data_type="GridPowerExport", value_type="average", sample_size="Min15"):
    headers = {
        "X-AuroraVision-Token": token,
        "Content-Type": "application/json"
    }

    # Get plant name for the CSV file
    folder_path = f"temp/{plant_name}"
    os.makedirs(folder_path, exist_ok=True)  # Ensure the folder is created

    # Prepare the filename with the correct path
    filename = os.path.join(folder_path, f"{plant_name}_grid.csv")
    
    # Clear the file before fetching new data
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["epoch_start", "datetime", "value", "units"])  # Header row

    data_url = (f"{BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/{value_type}"
                f"?sampleSize={sample_size}&startDate={start_date}&endDate={end_date}&timeZone=Asia/Bangkok")

    response = requests.get(data_url, headers=headers, auth=(USERNAME, PASSWORD))
    
    if response.status_code == 200:
        data = response.json()

        # Open the CSV file in append mode and write the data
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')  # Handle missing values gracefully
                units = entry.get('units', '')

                # Convert epoch to readable datetime in GMT+7
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    gmt_plus_7 = pytz.timezone('Asia/Bangkok')
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')

                    # Write each entry to the CSV
                    writer.writerow([epoch, datetime_str, value, units])
    else:
        print(f"Failed to fetch data for {start_date} to {end_date}: {response.status_code} - {response.text}")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def fetch_data_wrapper(token, inverters, start_date, end_date):
    """Wrapper for data fetching with retry mechanism"""
    try:
        # Verify and refresh token if needed
        if not verify_token(token):
            logger.info("Token invalid, refreshing...")
            token = authenticate()
            st.session_state.token = token
            
        return fetch_all_data_parallel(token, inverters, start_date, end_date)
    except Exception as e:
        logger.error(f"Error in fetch_data_wrapper: {str(e)}")
        raise

# Streamlit app
st.title("Real-Time Power Flow Visualization")

# Auto-refresh logic
if 8 <= datetime.now(gmt_plus_7).hour <= 17:
    st_autorefresh(interval=600_000, key="auto_refresh")

# Authenticate and get token
if "token" not in st.session_state:
    st.session_state.token = authenticate()

token = st.session_state.token

st.write("Fetching data for all plants today in 15-minute intervals...")

# Load inverters from file
with open('all_plants.json', 'r') as f:
    plants = json.load(f)

# Set date range
start_date = datetime.now().strftime("%Y%m%d")
end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

# Process and save data
for plant, entityID in list(plants.items()):
    fetch_inverter_power(token, entityID, plant, start_date, end_date)
    fetch_grid_power_export(token, entityID, plant, start_date, end_date)

# Generate power flow visualization for each plant
for plant, entityID in plants.items():
    power_path = f"temp/{plant}/{plant}_power.csv"
    grid_path = f"temp/{plant}/{plant}_grid.csv"
    
    try:
        power_df = pd.read_csv(power_path)
        grid_df = pd.read_csv(grid_path)
    except FileNotFoundError:
        continue

     # Merge power and grid data on epoch_start
    merged_df = pd.merge(
        power_df[['epoch_start', 'datetime', 'value']],
        grid_df[['epoch_start', 'value']],
        on='epoch_start',
        suffixes=('_power', '_grid'),
        how='outer'
    )

    # Find latest timestamp with both values available
    valid_data = merged_df.dropna(subset=['value_power', 'value_grid']).copy()

    if not valid_data.empty:
        st.markdown(f"### [{plant} Energy Balance](https://www.auroravision.net/dashboard/#{entityID})")
        # Get latest synchronized data point
        valid_data['Consumption'] = (valid_data['value_power'] - valid_data['value_grid']) / 1000  # Convert to kW
        valid_data['Consumption-fromGrid'] = valid_data['value_grid'].apply(lambda x: -x if x < 0 else 0)
        valid_data['Consumption-fromGrid'] = valid_data['Consumption-fromGrid'] / 1000  # Convert to kW
        valid_data['Solar-toGrid'] = valid_data['value_grid'].apply(lambda x: x if x > 0 else 0)
        valid_data['Solar-toGrid'] = valid_data['Solar-toGrid'] / 1000  # Convert to kW
        valid_data = valid_data.drop(columns=['value_grid']) # Drop the original grid column
        valid_data['Solar'] = valid_data['value_power']
        valid_data['Solar'] = valid_data['Solar'] / 1000  # Convert to kW
        valid_data = valid_data.drop(columns=['value_power']) # Drop the original power column
        valid_data['Consumption-fromSolar'] = valid_data['Solar'] - valid_data['Solar-toGrid']

        # Convert datetime to datetime type if it's string
        valid_data['datetime'] = pd.to_datetime(valid_data['datetime'])

        current_date = datetime.now(gmt_plus_7).date()

        # Create the figure
        # Create the figure
        fig = go.Figure()

        # Common parameters
        area_kwargs = {
            'line': dict(width=0),
            'stackgroup': 'source',
            'hovertemplate': '%{y:.2f} kW'
        }

        fig.add_trace(go.Scatter(
            x=valid_data['datetime'],
            y=valid_data['Consumption-fromSolar'],
            name='Consumption - from Solar',
            fillcolor='rgba(0, 128, 0, 0.7)',  # Green
            **area_kwargs
        ))

        fig.add_trace(go.Scatter(
            x=valid_data['datetime'],
            y=valid_data['Consumption-fromGrid'],
            name='Consumption - from Grid',
            fillcolor='rgba(255, 0, 0, 0.7)',  # Red
            **area_kwargs
        ))

        fig.add_trace(go.Scatter(
            x=valid_data['datetime'],
            y=valid_data['Solar-toGrid'],
            name='Solar - to Grid',
            fillcolor='rgba(255, 255, 0, 0.7)',  # Yellow
            **area_kwargs
        ))

        # Add total solar line
        fig.add_trace(go.Scatter(
            x=valid_data['datetime'],
            y=valid_data['Solar'],
            name='Solar (AC)',
            line=dict(color='blue', width=1.5),
            hovertemplate='%{y:.2f} kW'
        ))

        # Add total consumption line
        fig.add_trace(go.Scatter(
            x=valid_data['datetime'],
            y=valid_data['Consumption'],
            name='Consumption',
            line=dict(color='black', width=1.5, dash='dot'),
            hovertemplate='%{y:.2f} kW'
        ))

        # Update layout
        fig.update_layout(
            title='Energy Balance',
            xaxis_title='Time (Hours)',
            yaxis_title='Power (kW)',
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            ),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            xaxis=dict(
                gridcolor='rgba(128,128,128,0.2)',
                showgrid=True,
                range=[gmt_plus_7.localize(datetime.combine(current_date, datetime.strptime("06:00", "%H:%M").time())), gmt_plus_7.localize(datetime.combine(current_date, datetime.strptime("18:00", "%H:%M").time()))],
                tickformat='%H:%M',
                dtick=3600000*2  # Show tick every 2 hours (in milliseconds)
            ),
            yaxis=dict(
                gridcolor='rgba(128,128,128,0.2)',
                showgrid=True,
                range=[0, max(valid_data['Solar'].max(), 
                            valid_data['Consumption'].max()) * 1.1]
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        continue
