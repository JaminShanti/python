import logging
import plotly.express as px
import pandas as pd
import json
import os
import requests
from typing import Optional

# Set default renderer to kaleido (corrected typo)
import plotly.io as pio

pio.renderers.default = "kaleido"  # ✅ Correct name


class CoronaMapper:
    def __init__(self, date: str = "07-19-2020"):
        """Initialize CoronaMapper with logging configuration"""
        self.setup_logger()
        self.date = date
        self.state_names = [
            "Alabama", "Arkansas", "Arizona", "California", "Colorado", "Connecticut",
            "District of Columbia", "Delaware", "Florida", "Georgia", "Iowa", "Idaho",
            "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts",
            "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi",
            "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire",
            "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon",
            "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
            "Texas", "Utah", "Virginia", "Vermont", "Washington", "Wisconsin",
            "West Virginia", "Wyoming"
        ]

        # URLs for data sources
        self.missing_fips_url = (
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/"
            "csse_covid_19_data/csse_covid_19_daily_reports/03-30-2020.csv"
        )
        self.sample_url = (
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/"
            "csse_covid_19_data/csse_covid_19_daily_reports/07-19-2020.csv"
        )

        # GeoJSON file path and URL
        self.geojson_url = (
            "https://raw.githubusercontent.com/plotly/datasets/master/"
            "geojson-counties-fips.json"
        )
        self.geojson_path = "geojson-counties-fips.json"

        # Initialize data containers
        self.missing_fips: pd.DataFrame = pd.DataFrame()
        self.df_sample: pd.DataFrame = pd.DataFrame()
        self.df_sample_r: pd.DataFrame = pd.DataFrame()
        self.fig: px.choropleth = px.choropleth()

    def setup_logger(self):
        """Configure logging to only output to console"""
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        logging.info("Logger configured")

    def load_data(self) -> None:
        """Load raw data from CSV sources"""
        try:
            logging.info("Loading data from URLs...")
            self.missing_fips = pd.read_csv(self.missing_fips_url)
            self.df_sample = pd.read_csv(self.sample_url)
            logging.info("Successfully loaded data")
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise

    def prepare_data(self) -> None:
        """Process and combine dataframes"""
        try:
            logging.info("Preparing data...")
            # Calculate death percentages
            self.missing_fips['Death_Percentage'] = (
                    self.missing_fips['Deaths'] / self.missing_fips['Confirmed'] * 100
            )
            self.df_sample['Death_Percentage'] = (
                    self.df_sample['Death'] / self.df_sample['Confirmed'] * 100
            )

            # Filter by state names
            self.missing_fips_r = self.missing_fips[
                self.missing_fips['Province_State'].isin(self.state_names)
            ].fillna(0)
            self.df_sample_r = self.df_sample[
                self.missing_fips['Province_State'].isin(self.state_names)
            ].fillna(0)

            # Remove FIPS 0 and combine data
            self.missing_fips_r = self.missing_fips_r[
                ~self.missing_fips_r.FIPS.isin(self.df_sample_r.FIPS)
            ].dropna()
            self.df_sample_r = pd.concat([self.df_sample_r, self.missing_fips_r])

            # Remove FIPS=0
            self.df_sample_r = self.df_sample_r[~self.df_sample_r.FIPS.isin([0])]
            logging.info(f"Filtered {len(self.df_sample_r)} counties")
        except Exception as e:
            logging.error(f"Data preparation failed: {e}")
            raise

    def download_geojson(self) -> None:
        """Download GeoJSON file if it doesn't exist"""
        if not os.path.exists(self.geojson_path):
            logging.info(f"GeoJSON file not found. Downloading from {self.geojson_url}...")
            try:
                response = requests.get(self.geojson_url, timeout=10)
                response.raise_for_status()
                with open(self.geojson_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"Successfully downloaded {self.geojson_path}")
            except Exception as e:
                logging.error(f"Failed to download GeoJSON: {e}")
                raise

    def create_choropleth(self) -> None:
        """Create the choropleth map visualization"""
        try:
            self.download_geojson()  # Ensure GeoJSON file exists

            logging.info("Loading GeoJSON file...")
            with open(self.geojson_path, 'r') as f:
                counties = json.load(f)
            logging.info("GeoJSON loaded successfully")

            # Calculate state totals
            state_totals = self.df_sample_r.groupby('Province_State')['Death'].sum()
            self.df_sample_r['State_Deaths_Total'] = self.df_sample_r['Province_State'].map(state_totals)

            # Create death bins
            bins_edges = [1, 5, 25, 100, 500, float('inf')]
            bin_labels = ['1-5', '6-25', '26-100', '101-500', '500+']
            self.df_sample_r['Death_Bin'] = '0'

            mask_pos = self.df_sample_r['Death'] > 0
            if mask_pos.any():
                self.df_sample_r.loc[mask_pos, 'Death_Bin'] = pd.cut(
                    self.df_sample_r.loc[mask_pos, 'Death'],
                    bins=bins_edges,
                    labels=bin_labels,
                    right=True,
                    include_lowest=True
                ).astype(str)

            # Color mapping
            color_map = {
                '0': '#444444',
                '1-5': '#5b2a86',
                '6-25': '#3b4cc0',
                '26-100': '#1fa187',
                '101-500': '#55c667',
                '500+': '#fde725'
            }

            # Create choropleth
            self.fig = px.choropleth(
                self.df_sample_r,
                geojson=counties,
                locations='FIPS',
                color='Death_Bin',
                scope='usa',
                color_discrete_map=color_map,
                category_orders={'Death_Bin': ['0'] + bin_labels},
                labels={'Death_Bin': f'County deaths (bins) — {self.date}'},
                custom_data=['Province_State', 'Admin2', 'Death', 'FIPS', 'State_Deaths_Total', 'Death_Bin']
            )

            # Update traces
            self.fig.update_traces(
                marker_line_width=0.5,
                marker_line_color='rgb(255,255,255)',
                hovertemplate='State: %{customdata[0]}<br>'
                              'County: %{customdata[1]}<br>'
                              'FIPS: %{customdata[3]}<br>'
                              'County deaths: %{customdata[2]:,} (%{customdata[5]})<br>'
                              'State deaths total: %{customdata[4]:,}'
                              '<extra></extra>'
            )
            logging.info("Choropleth created successfully")
        except Exception as e:
            logging.error(f"Choropleth creation failed: {e}")
            raise

    def update_layout(self) -> None:
        """Update figure layout and styling"""
        try:
            self.fig.update_layout(
                legend_title_text=f'County deaths (bins) — {self.date}',
                legend_x=0,
                annotations=[{
                    'x': -0.12,
                    'y': 1.0,
                    'xref': 'paper',
                    'yref': 'paper',
                    'xanchor': 'left',
                    'showarrow': False
                }],
                template='plotly_dark'
            )
            logging.info("Layout updated successfully")
        except Exception as e:
            logging.error(f"Layout update failed: {e}")
            raise

    def save_figure(self, output_path: Optional[str] = None) -> None:
        """Save figure as PNG image using kaleido"""
        try:
            if output_path is None:
                output_path = f"covid_choropleth_{self.date}.png"

            self.fig.write_image(output_path, width=1200, height=800, scale=2, format="png")
            logging.info(f"Successfully saved {output_path}")
        except Exception as e:
            logging.error(f"Image export failed: {e}")
            raise

    def get_top_counties(self, n: int = 25) -> pd.DataFrame:
        """Get top N counties by deaths"""
        try:
            return self.df_sample_r.nlargest(n, 'Death')[[
                'Admin2', 'Province_State', 'Death'
            ]]
        except Exception as e:
            logging.error(f"Failed to get top counties: {e}")
            raise

    def generate_report(self) -> None:
        """Execute complete analysis workflow"""
        try:
            logging.info("Starting report generation...")
            self.load_data()
            self.prepare_data()
            self.create_choropleth()
            self.update_layout()
            self.save_figure()

            top_counties = self.get_top_counties()
            logging.info("Top counties by deaths:")
            logging.info(top_counties.to_string())

            logging.info("Report generation completed successfully")
        except Exception as e:
            logging.critical(f"Report generation failed: {e}")
            raise


if __name__ == "__main__":
    try:
        mapper = CoronaMapper(date="07-19-2020")
        mapper.generate_report()
    except Exception as e:
        logging.critical(f"Application failed: {e}")
