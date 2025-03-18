import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc) # from timestamp to datetime
                    local_time = utc_time.astimezone(gmt_plus_7) # from UTC to GMT+7
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S') # from datetime to string
                    results.append([epoch, datetime_str, serial, value, units]) 
            return serial, results
        else:
            logging.warning(f"Failed to fetch data for {plant_name} - Status: {response.status_code}")
            return serial, []
    except Exception as e:
        logging.error(f"Error fetching data for {plant_name}: {e}")
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

def fetch_grid_power_export(token, entityID, plant_name, start_date, end_date,
                            data_type="GridPowerExport", value_type="average", sample_size="Min15"):
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
                value = entry.get('value', '')  # Handle missing values gracefully
                units = entry.get('units', '')

                # Convert epoch to readable datetime in GMT+7
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc) # from timestamp to datetime
                    local_time = utc_time.astimezone(gmt_plus_7) # from UTC to GMT+7
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S') # from datetime to string
                    results.append([epoch, datetime_str, value, units])
            return results
        else:
            logging.warning(f"Failed to fetch data for {plant_name} - Status: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error fetching data for {plant_name}: {e}")
        return []

def fetch_inverter_power(token, entityID, plant_name, start_date, end_date, 
                        data_type="GenerationPower", value_type="average", sample_size="Min15"):
    # Prepare headers for API requests
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
                raw_value = entry.get('value', '')
                value = float(raw_value) if raw_value else None
                units = entry.get('units', '')

                # Convert epoch to readable datetime in GMT+7
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    results.append([epoch, datetime_str, value, units])
            return results
        else:
            logging.warning(f"Failed to fetch data for {plant_name} - Status: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error fetching data for {plant_name}: {e}")
        return []

def fetch_grid_power_export(token, entityID, plant_name, start_date, end_date,
                            data_type="GridPowerExport", value_type="average", sample_size="Min15"):
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
                raw_value = entry.get('value', '')
                value = float(raw_value) if raw_value else None
                units = entry.get('units', '')

                # Convert epoch to readable datetime in GMT+7
                if epoch:
                    utc_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(gmt_plus_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    results.append([epoch, datetime_str, value, units])
            return results
        else:
            logging.warning(f"Failed to fetch data for {plant_name} - Status: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error fetching data for {plant_name}: {e}")
        return []

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

with open('all_plants.json', 'r') as f:
    plants = json.load(f)

plant_names = list(inverters.keys())

# Dropdown for plant selection
selected_plant = st.selectbox("Select a Plant", plant_names)

# Generate date options for last 7 days (including today)
date_options = [(datetime.now() - timedelta(days=i)).date() for i in range(13, -1, -1)]
selected_date = st.selectbox("Select Date", date_options, format_func=lambda d: d.strftime("%Y-%m-%d"))

# Convert selected date to API format
start_date = selected_date.strftime("%Y%m%d")
end_date = (selected_date + timedelta(days=1)).strftime("%Y%m%d")

if st.button("Fetch and Visualize Data"):
    loggers = inverters.get(selected_plant, [])
    serials = logids.get(selected_plant, [])

    # Fetch data for the selected plant in parallel
    plant_data = fetch_plant_data_parallel(token, selected_plant, loggers, serials, start_date, end_date)
    for plant, entityID in list(plants.items()):
        if plant == selected_plant:
            entity = entityID
    power_df = fetch_inverter_power(token, entity, selected_plant, start_date, end_date)
    grid_df = fetch_grid_power_export(token, entity, selected_plant, start_date, end_date)

    power_df = pd.DataFrame(power_df, columns=["epoch_start", "datetime", "value", "units"])
    grid_df = pd.DataFrame(grid_df, columns=["epoch_start", "datetime", "value", "units"])

    merged_df = pd.merge(
        power_df[['epoch_start', 'datetime', 'value']],
        grid_df[['epoch_start', 'value']],
        on='epoch_start',
        suffixes=('_power', '_grid'),
        how='outer'
    )
    valid_data = merged_df.dropna(subset=['value_power', 'value_grid']).copy()
    # Process and save data
    df = pd.DataFrame()
    for entityID, results in plant_data:
        if results:
            df_logger = pd.DataFrame(results, columns=["epoch_start", "datetime", "serial", "value", "units"])
            if df_logger['value'].notnull().any():
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
        filtered_data['value'] = filtered_data['value'] / 1000  # Convert to kW

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
        # Set x-axis range to full day
        current_date = selected_date
        start_time = gmt_plus_7.localize(datetime.combine(current_date, datetime.strptime("06:00", "%H:%M").time()))
        end_time = gmt_plus_7.localize(datetime.combine(current_date, datetime.strptime("18:00", "%H:%M").time()))

        fig.update_xaxes(
            range=[start_time, end_time],
            tickformat="%H:%M",
            dtick=3600000*2, # Show tick every 2 hours (in milliseconds)
            title="Time (Hours)"
        )

        fig.update_yaxes(range=[0, 100], title="Power Output (kW)")
        fig.update_traces(hovertemplate='%{x} <br> Power: %{y:.2f} kW', mode='lines+markers')

        st.plotly_chart(fig, use_container_width=True)

    if not valid_data.empty:
        # st.markdown(f"### [{plant} Energy Balance](https://www.auroravision.net/dashboard/#{entityID})")
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

        current_date = selected_date

        # Create the figure
        fig = go.Figure()

        # Common parameters
        area_kwargs = {
            'line': dict(width=0),
            'stackgroup': 'source',
            'hovertemplate':'%{y:.2f} kW'
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
                range=[0, max(100, max(valid_data['Solar'].max(), valid_data['Consumption'].max()) * 1.1)]
            )
        )

        st.plotly_chart(fig, use_container_width=True)