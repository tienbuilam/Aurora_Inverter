import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import requests
import pytz
import os
import csv
import json
import math
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from functools import wraps

# Load secrets
BOT_TOKEN = st.secrets["telegram"]["bot_token"]
CHAT_ID = st.secrets["telegram"]["chat_id"]
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
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def fetch_current_date_parallel(token, entityID, plant_name, start_date, end_date,
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
                    results.append([epoch, datetime_str, entityID, value, units])
            return plant_name, entityID, results
        else:
            logging.warning(f"Failed to fetch data for {entityID} - Status: {response.status_code}")
            return plant_name, entityID, []
    except Exception as e:
        logging.error(f"Error fetching data for {entityID}: {e}")
        return plant_name, entityID, []

# Function to fetch all data in parallel
def fetch_all_data_parallel(token, inverters, start_date, end_date):
    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                fetch_current_date_parallel, token, logger, plant_name, start_date, end_date
            )
            for plant_name, loggers in inverters.items()
            for logger in loggers
        ]

        for future in as_completed(futures):
            all_results.append(future.result())

    return all_results

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

def send_telegram_alert(message):
    if 8 <= datetime.now(gmt_plus_7).hour <= 16:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            
            payload = {
                "chat_id": CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Telegram send failed: {str(e)}")
            return False
    else:
        return False

def check_inverter_time(data, plant_name):
    data['datetime'] = pd.to_datetime(data['datetime'])
    time = data[data['value'].notnull()]['datetime'].iloc[-1]
    datetime_obj = datetime.now(gmt_plus_7)

    # Ensure both have the same timezone (GMT+7)
    datetime_obj = datetime_obj.astimezone(pytz.timezone('Asia/Bangkok'))  # Convert datetime to GMT+7
    timestamp_obj = time.tz_localize('Asia/Bangkok')  # Ensure the Timestamp is localized to GMT+7

    if datetime_obj - timedelta(minutes=30) > timestamp_obj:
        inverter_id = data['entityID'].iloc[0]
        msg = f"{plant_name}, inverter {inverter_id} outdated.\nLast update: {timestamp_obj.strftime('%Y-%m-%d %H:%M')}"
        st.warning(msg, icon="⚠️")
        send_telegram_alert(msg)
        return False
    else:
        return True

def compare_latest_inverter_power(data, plant_name):    
    time = data[data['value'].notnull()]['datetime'].iloc[-1]
    data = data[data['datetime'] == time].sort_values(by='value', ascending=False)
    inverter_ids = data['entityID'].unique()
    if data['value'].iloc[0] > 50:
        for i in range(1, len(inverter_ids)):
            if data['value'].iloc[i] < data['value'].iloc[0] * 0.25:
                msg = f"{plant_name}, inverter {inverter_ids[i]} is underperforming with {round(data['value'].iloc[i], 2)} kW.\nTime: {time.strftime('%Y-%m-%d %H:%M')}"
                st.warning(msg, icon="⚠️")
                send_telegram_alert(msg)
    else:
        return None

def check_low_power_period(data, plant_name):
    inverter_id = data['entityID'].iloc[0]
    time = data[data['value'].notnull()]['datetime']
    value = data[data['value'].notnull()]['value']
    if value.iloc[-1] < 5000 and value.size > 3:
        if value.iloc[-2] < 5000 and value.iloc[-3] < 5000:
            msg = f"{plant_name}, inverter {inverter_id} detects low power.\nFrom {time.iloc[-3].strftime('%Y-%m-%d %H:%M')} to {time.iloc[-1].strftime('%Y-%m-%d %H:%M')}"
            st.warning(msg, icon="⚠️")
            send_telegram_alert(msg)
        elif value.iloc[-2] > 50000:
            msg = f"{plant_name}, inverter {inverter_id} detects high power drop.\nFrom {time.iloc[-2].strftime('%Y-%m-%d %H:%M')} to {time.iloc[-1].strftime('%Y-%m-%d %H:%M')}"
            st.warning(msg, icon="⚠️")
            send_telegram_alert(msg)

# Streamlit app
st.title("All Plant Power Output Visualization")

# Auto-refresh logic
if 8 <= datetime.now(gmt_plus_7).hour <= 16:
    st_autorefresh(interval=600_000, key="auto_refresh")

# Authenticate and get token
if "token" not in st.session_state:
    st.session_state.token = authenticate()

token = st.session_state.token

st.write("Fetching data for all plants today in 15-minute intervals...")

# Load inverters from file
with open('all_inverters.json', 'r') as f:
    inverters = json.load(f)

# Set date range
start_date = datetime.now().strftime("%Y%m%d")
end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

# Fetch data in parallel
all_data = fetch_all_data_parallel(token, inverters, start_date, end_date)

# Process and save data
for plant_name, entityID, results in all_data:
    if results:
        folder_path = f"temp/{plant_name}"
        os.makedirs(folder_path, exist_ok=True)
        filename = os.path.join(folder_path, f"{entityID}.csv")
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["epoch_start", "datetime", "entityID", "value", "units"])
            writer.writerows(results)

st.success("Data fetching completed. Generating graphs...")

# Generate graphs for each plant
for plant_name, loggers in inverters.items():
    df = pd.DataFrame()
    for logger in loggers:
        filename = f"temp/{plant_name}/{logger}.csv"
        if os.path.exists(filename):
            df_logger = pd.read_csv(filename)
            if df_logger['value'].notnull().any():
                if check_inverter_time(df_logger, plant_name):
                    check_low_power_period(df_logger, plant_name)
                df = pd.concat([df, df_logger], ignore_index=True)

    if not df.empty:
        filtered_data = df.dropna(subset=['value']).copy()
        filtered_data['datetime'] = pd.to_datetime(filtered_data['datetime'])
        filtered_data = filtered_data.sort_values(by='datetime')

        # Introduce None for breaks in continuity
        time_diff = filtered_data['datetime'].diff().dt.total_seconds()
        threshold = 15 * 60
        filtered_data.loc[time_diff > threshold, 'value'] = None
        filtered_data['value'] = filtered_data['value'] / 1000  # Convert to kW

        compare_latest_inverter_power(filtered_data, plant_name)

        with open('all_plants.json', 'r') as f:
            plants = json.load(f)

        entity = None
        for plant, entityID in list(plants.items()):
            if plant == plant_name:
                entity = entityID

        # Render a clickable title as Markdown in Streamlit
        url = f"https://www.auroravision.net/dashboard/#{entity}"  # Replace with your desired URL
        title_with_link = f"[{plant_name} AC Output: Power]({url})"
        st.markdown(f"### {title_with_link}")

        # Plot graph
        fig = px.line(
            filtered_data,
            x='datetime',
            y='value',
            color='entityID',
            title=f"{plant_name} Power Output",
            labels={'datetime': 'Time', 'value': 'Power Output (kW)'},
            template='plotly_white'
        )
        # Set x-axis range to full day
        current_date = datetime.now(gmt_plus_7).date()
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
        
    else:
        continue
