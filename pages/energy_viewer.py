import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests
import pytz
import os
import csv
import json
import logging
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from streamlit_autorefresh import st_autorefresh

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Timezone configuration
GMT_PLUS_7 = pytz.timezone('Asia/Bangkok')

# Page config
st.set_page_config(page_title="Energy Viewer", layout="wide")


class EnergyBalanceApp:
    def __init__(self):
        # Configuration loading
        self.load_configurations()

        # Authentication
        self.token = None

    def load_configurations(self):
        """Load configuration files"""
        try:
            # Load plant data from JSON files
            with open('all_plants.json', 'r', encoding='utf-8') as f:
                self.plants = json.load(f)

            # Load secrets
            self.API_KEY = st.secrets["aurora"]["api_key"]
            self.USERNAME = st.secrets["aurora"]["username"]
            self.PASSWORD = st.secrets["aurora"]["password"]
            self.BASE_URL = st.secrets["aurora"]["base_url"]

        except FileNotFoundError as e:
            logger.error(f"Configuration file not found: {e}")
            self.plants = []
        except Exception as e:
            logger.error(f"Error loading configurations: {e}")
            self.plants = []

    def check_ppa(self, data, plant_name):
        """Check if plant exceeds PPA limit"""
        try:
            df = pd.read_csv('inverter.csv')
            df.drop(columns=['PPA'], inplace=True)
            names = df['Plant Name'].values
            ppas = df['PPAx0.8'].values

            for name, ppa in zip(names, ppas):
                if (name == plant_name) and (ppa is not None):
                    if data['Solar-toGrid'].iloc[-1] > ppa:
                        st.warning(
                            f"Plant **{name}** has exceeded the PPA limit of {ppa:.2f} kWh. Current value: {data['Solar-toGrid'].iloc[-1]:.2f} kWh", icon="⚠️")
                        return True
                    return False
        except Exception as e:
            logger.error(f"Error checking PPA for {plant_name}: {e}")
            return False

    def calculate_next_refresh_time(self, current_time):
        """Calculate next refresh time at 15-minute intervals"""
        minutes = (current_time.minute // 15) * 15
        next_refresh = current_time.replace(
            minute=minutes, second=0, microsecond=0)

        if next_refresh == current_time:
            next_refresh += timedelta(minutes=15)

        while next_refresh <= current_time:
            next_refresh += timedelta(minutes=15)

        return next_refresh + timedelta(minutes=3)

    def auto_refresh_timer(self):
        """Handle auto-refresh logic"""
        current_time = datetime.now(GMT_PLUS_7)

        # Refresh only during working hours (7:00 AM to 4:00 PM)
        if 7 <= current_time.hour <= 16:
            next_refresh = self.calculate_next_refresh_time(current_time)
            remaining_seconds = int(
                (next_refresh - current_time).total_seconds())

            st_autorefresh(interval=remaining_seconds *
                           1000, key="precise_auto_refresh")
            st.text(
                f"Next refresh at: {next_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

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
    def fetch_plant_data(self, token, entityID, plant_name, data_type):
        """Fetch data for a plant"""
        if not token:
            logger.error("No valid token available")
            return None

        headers = {
            "X-AuroraVision-Token": token,
            "Content-Type": "application/json"
        }

        # Get today's date
        today = datetime.now(GMT_PLUS_7).strftime('%Y%m%d')
        tomorrow = (datetime.now(GMT_PLUS_7) +
                    timedelta(days=1)).strftime('%Y%m%d')

        data_url = (f"{self.BASE_URL}/v1/stats/power/timeseries/{entityID}/{data_type}/average"
                    f"?sampleSize=Min15&startDate={today}&endDate={tomorrow}&timeZone=Asia/Bangkok")

        try:
            response = requests.get(
                data_url,
                headers=headers,
                auth=(self.USERNAME, self.PASSWORD),
                timeout=30  # Add timeout
            )
            # Check for other errors
            response.raise_for_status()

            data = response.json()
            if not data.get('result'):
                logger.warning(f"No data returned for {plant_name}")
                return None

            results = []
            for entry in data.get('result', []):
                epoch = entry.get('start')
                value = entry.get('value', '')
                units = entry.get('units', '')

                if epoch:
                    utc_time = datetime.utcfromtimestamp(
                        epoch).replace(tzinfo=pytz.utc)
                    local_time = utc_time.astimezone(GMT_PLUS_7)
                    datetime_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                    results.append([epoch, datetime_str, value, units])

            if results:
                data = [(plant_name, results)]
                self.save_plant_data(data, data_type)
                return data
            return None

        except requests.Timeout:
            logger.error(f"Request timeout for {plant_name}")
            return None
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {plant_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {plant_name}: {str(e)}")
            return None

    def save_plant_data(self, data, data_type):
        """Save fetched plant data to CSV files"""
        for plant_name, results in data:
            if results:
                folder_path = f"temp/{plant_name}"
                os.makedirs(folder_path, exist_ok=True)
                if data_type == "GenerationPower":
                    filename = os.path.join(
                        folder_path, f"{plant_name}_power.csv")
                elif data_type == "GridPowerExport":
                    filename = os.path.join(
                        folder_path, f"{plant_name}_grid.csv")

                with open(filename, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        ["epoch_start", "datetime", "value", "units"])
                    writer.writerows(results)

    def create_energy_balance_plot(self, data, plant_name, entityID):
        """Create energy balance visualization for a single plant"""
        if data.empty:
            return None

        # Check PPA before creating plot
        self.check_ppa(data, plant_name)

        # Create the figure
        fig = go.Figure()

        # Common parameters
        area_kwargs = {
            'line': dict(width=0),
            'stackgroup': 'source',
            'hovertemplate': '%{y:.2f} kW'
        }

        # Add stacked area traces
        fig.add_trace(go.Scatter(
            x=data['datetime'],
            y=data['Consumption-fromSolar'],
            name='Consumption - from Solar',
            fillcolor='rgba(0, 128, 0, 0.7)',  # Green
            **area_kwargs
        ))

        fig.add_trace(go.Scatter(
            x=data['datetime'],
            y=data['Consumption-fromGrid'],
            name='Consumption - from Grid',
            fillcolor='rgba(255, 0, 0, 0.7)',  # Red
            **area_kwargs
        ))

        fig.add_trace(go.Scatter(
            x=data['datetime'],
            y=data['Solar-toGrid'],
            name='Solar - to Grid',
            fillcolor='rgba(255, 255, 0, 0.7)',  # Yellow
            **area_kwargs
        ))

        # Add total solar line
        fig.add_trace(go.Scatter(
            x=data['datetime'],
            y=data['Solar'],
            name='Solar (AC)',
            line=dict(color='blue', width=1.5),
            hovertemplate='%{y:.2f} kW'
        ))

        # Add total consumption line
        fig.add_trace(go.Scatter(
            x=data['datetime'],
            y=data['Consumption'],
            name='Consumption',
            line=dict(color='black', width=1.5, dash='dot'),
            hovertemplate='%{y:.2f} kW'
        ))

        # Set x-axis range for business hours
        current_date = datetime.now(GMT_PLUS_7).date()
        start_time = GMT_PLUS_7.localize(datetime.combine(
            current_date, datetime.strptime("06:00", "%H:%M").time()))
        end_time = GMT_PLUS_7.localize(datetime.combine(
            current_date, datetime.strptime("18:00", "%H:%M").time()))

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
                range=[start_time, end_time],
                tickformat='%H:%M',
                dtick=3600000*2  # Show tick every 2 hours
            ),
            yaxis=dict(
                gridcolor='rgba(128,128,128,0.2)',
                showgrid=True,
                range=[0, max(100, max(data['Solar'].max(),
                              data['Consumption'].max()) * 1.1)]
            )
        )

        return fig

    def display_metrics(self, data, plant_name):
        """Display current power metrics for a single plant"""
        # Get the latest timestamp where we have data
        latest_time = data[data['Solar'].notnull()]['datetime'].max()
        latest_data = data[data['datetime'] == latest_time]

        col1, col2, col3 = st.columns(3)
        with col1:
            current_solar = latest_data['Solar'].iloc[0] if not latest_data.empty else 0
            st.metric("Current Solar Power", f"{current_solar:.2f} kW")
        with col2:
            current_grid_exp = latest_data['Solar-toGrid'].iloc[0] if not latest_data.empty else 0
            st.metric("Current Grid Export", f"{current_grid_exp:.2f} kW")
        with col3:
            current_grid_imp = latest_data['Consumption-fromGrid'].iloc[0] if not latest_data.empty else 0
            st.metric("Current Grid Import", f"{current_grid_imp:.2f} kW")

    def process_and_visualize_data(self):
        """Process fetched data and create visualizations"""
        # First authenticate
        token = self.authenticate()
        if not token:
            return

        # Fetch and process data for each plant
        with st.spinner("Fetching data for all plants..."):
            for plant_name, entityID in self.plants.items():
                # Fetch data
                self.fetch_plant_data(
                    token, entityID, plant_name, "GenerationPower")
                self.fetch_plant_data(
                    token, entityID, plant_name, "GridPowerExport")

                # Process data for the plant
                power_path = f"temp/{plant_name}/{plant_name}_power.csv"
                grid_path = f"temp/{plant_name}/{plant_name}_grid.csv"

                try:
                    power_df = pd.read_csv(power_path)
                    grid_df = pd.read_csv(grid_path)

                    # Merge power and grid data
                    merged_df = pd.merge(
                        power_df[['epoch_start', 'datetime', 'value']],
                        grid_df[['epoch_start', 'value']],
                        on='epoch_start',
                        suffixes=('_power', '_grid'),
                        how='outer'
                    )

                    valid_data = merged_df.dropna(
                        subset=['value_power', 'value_grid']).copy()
                    if not valid_data.empty:
                        # Add clickable title with link to AuroraVision
                        st.markdown(
                            f"### [{plant_name} Energy Balance](https://www.auroravision.net/dashboard/#{entityID})")

                        # Calculate metrics
                        valid_data['Consumption'] = (
                            valid_data['value_power'] - valid_data['value_grid']) / 1000
                        valid_data['Consumption-fromGrid'] = valid_data['value_grid'].apply(
                            lambda x: -x if x < 0 else 0) / 1000
                        valid_data['Solar-toGrid'] = valid_data['value_grid'].apply(
                            lambda x: x if x > 0 else 0) / 1000
                        valid_data['Solar'] = valid_data['value_power'] / 1000
                        valid_data['Consumption-fromSolar'] = valid_data['Solar'] - \
                            valid_data['Solar-toGrid']
                        valid_data['datetime'] = pd.to_datetime(
                            valid_data['datetime'])

                        # Create and display plot
                        fig = self.create_energy_balance_plot(
                            valid_data, plant_name, entityID)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                            self.display_metrics(valid_data, plant_name)
                            st.markdown("---")  # Add separator between plants

                except FileNotFoundError:
                    logger.warning(f"Data files not found for {plant_name}")
                    continue
                except Exception as e:
                    logger.error(
                        f"Error processing data for {plant_name}: {e}")
                    continue

    def run(self):
        """Main application logic"""
        st.title("All Factories Energy Balance")

        # Add smart auto-refresh
        self.auto_refresh_timer()

        # Process and visualize data
        self.process_and_visualize_data()


def main():
    app = EnergyBalanceApp()
    app.run()


if __name__ == "__main__":
    main()
