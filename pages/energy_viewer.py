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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

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

    def load_configurations(self):
        """Load configuration files"""
        try:
            # Load inverters and serials from JSON files
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
    def fetch_inverter_data(self, token, entityID, plant_name, start_date, end_date, 
                            data_type):
        # Prepare headers for API requests
        headers = {
            "X-AuroraVision-Token": token,
            "Content-Type": "application/json"
        }

        data_url = (f"{self.BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/average"
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
                    results.append([epoch, datetime_str, value, units])
            
            data = [(plant_name, results)]
            self.save_inverter_data(data, data_type)
        
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {entityID}: {e}")
            data = [(plant_name, [])]
            self.save_inverter_data(data, data_type)

    def save_inverter_data(self, data, data_type):
        """Save fetched inverter data to CSV files"""
        for plant_name, results in data:
            if results:
                folder_path = f"temp/{plant_name}"
                os.makedirs(folder_path, exist_ok=True)
                if data_type == "GenerationPower":
                    filename = os.path.join(folder_path, f"{plant_name}_power.csv")
                elif data_type == "GridPowerExport":
                    filename = os.path.join(folder_path, f"{plant_name}_grid.csv")
                
                with open(filename, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(["epoch_start", "datetime", "value", "units"])
                    writer.writerows(results)

    def check_ppa(self, data, plant_name):
        df = pd.read_csv('inverter.csv')
        df.drop(columns=['PPA'], inplace=True)
        names = df['Plant Name'].values
        ppas = df['PPAx0.8'].values

        for name, ppa in zip(names, ppas):
            if (name == plant_name) & (ppa != None):
                if data['Solar-toGrid'].iloc[-1] > ppa:
                    st.warning(f"Plant **{name}** has exceeded the PPA limit of {ppa.round(2)} kWh. Current value: {data['Solar-toGrid'].iloc[-1].round(2)} kWh", icon="⚠️")
                    return True
                return False
    
    def process_and_visualize_data(self):
        """Process fetched data and create visualizations"""
        # Generate power flow visualization for each plant
        for plant_name, entityID in self.plants.items():
            power_path = f"temp/{plant_name}/{plant_name}_power.csv"
            grid_path = f"temp/{plant_name}/{plant_name}_grid.csv"
            
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
                st.markdown(f"### [{plant_name} Energy Balance](https://www.auroravision.net/dashboard/#{entityID})")
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

                # Plot graph
                self.plot_power_output(valid_data, plant_name)

    def plot_power_output(self, valid_data, plant_name):
        self.check_ppa(valid_data, plant_name)

        current_date = datetime.now(GMT_PLUS_7).date()

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
                range=[GMT_PLUS_7.localize(datetime.combine(current_date, datetime.strptime("06:00", "%H:%M").time())), GMT_PLUS_7.localize(datetime.combine(current_date, datetime.strptime("18:00", "%H:%M").time()))],
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
        st.set_page_config(page_title="Energy Viewer", layout="centered")
        st.title("Solar Plant Power Flow Visualization")
        self.authenticate()
        # Apply auto-refresh timer
        self.auto_refresh_timer()

        # Set date range
        start_date = datetime.now().strftime("%Y%m%d")
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

        for plant_name, entityID in self.plants.items():
            power_data = self.fetch_inverter_data(self.token, entityID, plant_name, start_date, end_date, data_type="GenerationPower")
            grid_data = self.fetch_inverter_data(self.token, entityID, plant_name, start_date, end_date, data_type="GridPowerExport")

        # Fetch data in parallel
        st.write("Fetching data for all plants today in 15-minute intervals...")

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
