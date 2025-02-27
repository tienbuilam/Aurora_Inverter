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

# Message tracking system
MESSAGE_HISTORY_FILE = "message_history.json"

def load_message_history():
    """Load message history from file"""
    if os.path.exists(MESSAGE_HISTORY_FILE):
        try:
            with open(MESSAGE_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading message history: {e}")
            return {}
    return {}

def save_message_history(history):
    """Save message history to file"""
    try:
        with open(MESSAGE_HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        logging.error(f"Error saving message history: {e}")

def clean_old_messages(history):
    """Remove messages older than 15 minutes"""
    current_time = datetime.now(gmt_plus_7).timestamp()
    cutoff_time = current_time - (15 * 60)  # 15 minutes ago
    
    return {
        key: value for key, value in history.items() 
        if value.get('timestamp', 0) > cutoff_time
    }

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
            return plant_name, serial, results
        else:
            logging.warning(f"Failed to fetch data for {serial} - Status: {response.status_code}")
            return plant_name, serial, []
    except Exception as e:
        logging.error(f"Error fetching data for {serial}: {e}")
        return plant_name, serial, []

# Function to fetch all data in parallel
def fetch_all_data_parallel(token, inverters, serials, start_date, end_date):
    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        # Iterate through each plant
        for plant_name in inverters:
            # Get corresponding inverters and serials for this plant
            plant_inverters = inverters.get(plant_name, [])
            plant_serials = serials.get(plant_name, [])

            # Create futures for each inverter-serial pair
            for inverter_id, serial in zip(plant_inverters, plant_serials):
                futures.append(
                    executor.submit(
                        fetch_current_date_parallel,  # Your existing function
                        token,
                        inverter_id,
                        serial,
                        plant_name,
                        start_date,
                        end_date
                    )
                )

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_results.append(result)
            except Exception as e:
                print(f"Error processing future: {str(e)}")

    return all_results

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def fetch_data_wrapper(token, inverters, serials, start_date, end_date):
    """Wrapper for data fetching with retry mechanism"""
    try:
        # Verify and refresh token if needed
        if not verify_token(token):
            logger.info("Token invalid, refreshing...")
            token = authenticate()
            st.session_state.token = token
            
        return fetch_all_data_parallel(token, inverters, serials, start_date, end_date)
    except Exception as e:
        logger.error(f"Error in fetch_data_wrapper: {str(e)}")
        raise

def verify_token(token):
    """Placeholder for token verification function"""
    # Add your token verification logic here
    return True if token else False

def send_telegram_alert(message, issue_id, issue_details=None):
    """
    Send alert to Telegram with tracking to avoid duplicates
    
    Parameters:
    - message: The alert message to send
    - issue_id: Unique identifier for this specific issue (e.g., "plant_name_inverter_id_issue_type")
    - issue_details: Additional details about the issue for comparison
    
    Returns:
    - True if message was sent, False otherwise
    """
    if 8 <= datetime.now(gmt_plus_7).hour <= 16:
        # Load message history
        message_history = load_message_history()
        
        # Clean old messages first
        message_history = clean_old_messages(message_history)
        
        current_time = datetime.now(gmt_plus_7).timestamp()
        
        # Check if this issue already exists in history
        if issue_id in message_history:
            last_sent_time = message_history[issue_id].get('timestamp', 0)
            last_details = message_history[issue_id].get('details', '')
            
            # If the same issue was sent less than 15 minutes ago, don't send again
            if current_time - last_sent_time < 15 * 60:
                # If the details are the same, don't send
                if last_details == issue_details:
                    return False
            
            # If it's been more than 15 minutes or details changed, update and send
            message_history[issue_id] = {
                'timestamp': current_time,
                'details': issue_details,
                'message': message
            }
        else:
            # New issue, add to history
            message_history[issue_id] = {
                'timestamp': current_time,
                'details': issue_details,
                'message': message
            }
        
        # Save updated history
        save_message_history(message_history)
        
        # Send the message
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
    """Check if inverter data is outdated"""
    data['datetime'] = pd.to_datetime(data['datetime'])
    time = data[data['value'].notnull()]['datetime'].iloc[-1]
    datetime_obj = datetime.now(gmt_plus_7)

    # Ensure both have the same timezone (GMT+7)
    datetime_obj = datetime_obj.astimezone(pytz.timezone('Asia/Bangkok'))
    timestamp_obj = time.tz_localize('Asia/Bangkok')

    serial_id = data['serial'].iloc[0]
    issue_id = f"{plant_name}_{serial_id}_outdated"
    
    if datetime_obj - timedelta(minutes=30) > timestamp_obj:
        timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M')
        msg = f"{plant_name}, inverter {serial_id} outdated.\nLast update: {timestamp_str}"
        details = f"last_update:{timestamp_str}"
        
        st.warning(msg, icon="⚠️")
        send_telegram_alert(msg, issue_id, details)
        return False
    else:
        # Check if we need to send a resolution message
        message_history = load_message_history()
        issue_id = f"{plant_name}_{serial_id}_outdated"
        
        if issue_id in message_history:
            # Issue is now resolved
            resolution_msg = f"{plant_name}, inverter {serial_id} is now up-to-date."
            resolution_id = f"{issue_id}_resolved"
            send_telegram_alert(resolution_msg, resolution_id)
            
            # Remove the issue from history
            message_history.pop(issue_id, None)
            save_message_history(message_history)
            
        return True

def compare_latest_inverter_power(data, plant_name):
    """Compare power output of inverters"""
    time = data[data['value'].notnull()]['datetime'].iloc[-1]
    data = data[data['datetime'] == time].sort_values(by='value', ascending=False)
    serial_ids = data['serial'].unique()
    
    if data['value'].iloc[0] > 50:
        for i in range(1, len(serial_ids)):
            underperforming_serial = serial_ids[i]
            issue_id = f"{plant_name}_{underperforming_serial}_underperforming"
            
            if data['value'].iloc[i] < data['value'].iloc[0] * 0.25:
                current_value = round(data['value'].iloc[i], 2)
                time_str = time.strftime('%Y-%m-%d %H:%M')
                msg = f"{plant_name}, inverter {underperforming_serial} is underperforming with {current_value} kW.\nTime: {time_str}"
                details = f"value:{current_value},time:{time_str}"
                
                st.warning(msg, icon="⚠️")
                send_telegram_alert(msg, issue_id, details)
            else:
                # Check if we need to send a resolution message
                message_history = load_message_history()
                if issue_id in message_history:
                    # Issue is now resolved
                    current_value = round(data['value'].iloc[i], 2)
                    resolution_msg = f"{plant_name}, inverter {underperforming_serial} is now performing normally at {current_value} kW."
                    resolution_id = f"{issue_id}_resolved"
                    send_telegram_alert(resolution_msg, resolution_id)
                    
                    # Remove the issue from history
                    message_history.pop(issue_id, None)
                    save_message_history(message_history)
    else:
        return None

def check_low_power_period(data, plant_name):
    """Check for low power output and high power drop"""
    serial_id = data['serial'].iloc[0]
    time = data[data['value'].notnull()]['datetime']
    value = data[data['value'].notnull()]['value']
    
    if value.iloc[-1] < 5000 and value.size > 3:
        if value.iloc[-2] < 5000 and value.iloc[-3] < 5000:
            start_time = time.iloc[-3].strftime('%Y-%m-%d %H:%M')
            end_time = time.iloc[-1].strftime('%Y-%m-%d %H:%M')
            issue_id = f"{plant_name}_{serial_id}_low_power"
            details = f"start:{start_time},end:{end_time},value:{value.iloc[-1]}"
            
            msg = f"{plant_name}, inverter {serial_id} detects low power.\nFrom {start_time} to {end_time}"
            st.warning(msg, icon="⚠️")
            send_telegram_alert(msg, issue_id, details)
        elif value.iloc[-2] > 50000:
            start_time = time.iloc[-2].strftime('%Y-%m-%d %H:%M')
            end_time = time.iloc[-1].strftime('%Y-%m-%d %H:%M')
            issue_id = f"{plant_name}_{serial_id}_power_drop"
            details = f"start:{start_time},end:{end_time},from:{value.iloc[-2]},to:{value.iloc[-1]}"
            
            msg = f"{plant_name}, inverter {serial_id} detects high power drop.\nFrom {start_time} to {end_time}"
            st.warning(msg, icon="⚠️")
            send_telegram_alert(msg, issue_id, details)
    else:
        # Check if we need to send resolution messages
        message_history = load_message_history()
        low_power_id = f"{plant_name}_{serial_id}_low_power"
        power_drop_id = f"{plant_name}_{serial_id}_power_drop"
        
        issues_resolved = []
        if low_power_id in message_history:
            issues_resolved.append((low_power_id, "low power"))
        if power_drop_id in message_history:
            issues_resolved.append((power_drop_id, "power drop"))
            
        for issue_id, issue_type in issues_resolved:
            resolution_msg = f"{plant_name}, inverter {serial_id} has recovered from {issue_type}. Current value: {round(value.iloc[-1]/1000, 2)} kW"
            resolution_id = f"{issue_id}_resolved"
            send_telegram_alert(resolution_msg, resolution_id)
            
            # Remove the issue from history
            message_history.pop(issue_id, None)
            
        if issues_resolved:
            save_message_history(message_history)

# Streamlit app
st.set_page_config(page_title="Alert Page", layout="centered")

st.title("All Plant Power Output Alert")

# Auto-refresh logic
if 8 <= datetime.now(gmt_plus_7).hour <= 16:
    st_autorefresh(interval=600_000, key="auto_refresh")

# Authenticate and get token
if "token" not in st.session_state:
    st.session_state.token = authenticate()

token = st.session_state.token

st.write("Notification will be sent if any issues are detected from 8am to 5pm.")

# Load inverters from file
with open('all_inverters.json', 'r') as f:
    inverters = json.load(f)

with open('all_serial.json', 'r') as f:
    serials = json.load(f)

# Set date range
start_date = datetime.now().strftime("%Y%m%d")
end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

# Fetch data in parallel
all_data = fetch_all_data_parallel(token, inverters, serials, start_date, end_date)

# Process and save data
for plant_name, entityID, results in all_data:
    if results:
        folder_path = f"temp/{plant_name}"
        os.makedirs(folder_path, exist_ok=True)
        filename = os.path.join(folder_path, f"{entityID}.csv")
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["epoch_start", "datetime", "serial", "value", "units"])
            writer.writerows(results)

st.success("Data fetching completed. Errors will be displayed below.")

# Generate graphs for each plant
for plant_name, serials in serials.items():
    df = pd.DataFrame()
    drop = []
    for serial in serials:
        filename = f"temp/{plant_name}/{serial}.csv"
        if os.path.exists(filename):
            df_logger = pd.read_csv(filename)
            if df_logger['value'].notnull().any():
                if check_inverter_time(df_logger, plant_name):
                    check_low_power_period(df_logger, plant_name)
                df = pd.concat([df, df_logger], ignore_index=True)
            else:
                drop.append([plant_name, serial])

    if not df.empty:
        for plant_name, serial in drop:  # Check for deactivated inverters
            issue_id = f"{plant_name}_{serial}_deactivated"
            msg = f"{plant_name}, inverter {serial} is deactivated."
            st.warning(msg, icon="⚠️")
            send_telegram_alert(msg, issue_id, "deactivated")
            
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
    else:
        continue

# Add cleanup job at the end of the script to remove old messages
# This ensures that issues that no longer appear will be removed
message_history = load_message_history()
message_history = clean_old_messages(message_history)
save_message_history(message_history)