import logging
import plotly.express as px
import pandas as pd
import json
import os
import requests
import argparse
from typing import Optional
import plotly.io as pio

# Set default renderer to kaleido
# Attempt to set Kaleido as the default renderer; ignore if not available
try:
    pio.renderers.default = "kaleido"
except Exception:
    # Fallback to the default Plotly renderer
    pass

class CoronaMapper:
    """
    A class to generate a choropleth map of COVID-19 deaths by county in the US.
    """
    def __init__(self, date: str = "07-19-2020", output_dir: str = None, data_dir: str = None):
        self.setup_logger()
        self.date = date
        # Determine base cache directory (environment variable or script location)
        base_cache = os.getenv("CACHE_ROOT", os.path.abspath(os.path.dirname(__file__)))
        # Use provided dirs or default to subfolders within the cache root
        self.output_dir = output_dir or os.path.join(base_cache, "corona_output")
        self.data_dir = data_dir or os.path.join(base_cache, "corona_data")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        self.logger.info(f"Output directory set to: {self.output_dir}")
        self.logger.info(f"Data directory set to: {self.data_dir}")

        # List of US States
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
            f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/"
            f"csse_covid_19_data/csse_covid_19_daily_reports/{self.date}.csv"
        )

        # GeoJSON file path and URL
        self.geojson_url = (
            "https://raw.githubusercontent.com/plotly/datasets/master/"
            "geojson-counties-fips.json"
        )
        self.geojson_path = os.path.join(self.data_dir, "corona_cache/geojson-counties-fips.json") # Updated path

        # Initialize data containers
        self.missing_fips: pd.DataFrame = pd.DataFrame()
        self.df_sample: pd.DataFrame = pd.DataFrame()
        self.df_combined: pd.DataFrame = pd.DataFrame()
        self.fig = None

    def setup_logger(self):
        """Configure logging to output to console"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_data(self) -> None:
        """Load raw data from CSV sources"""
        try:
            self.logger.info(f"Loading reference data from {self.missing_fips_url}...")
            self.missing_fips = pd.read_csv(self.missing_fips_url)
            
            self.logger.info(f"Loading daily report for {self.date} from {self.sample_url}...")
            self.df_sample = pd.read_csv(self.sample_url)
            
            self.logger.info("Successfully loaded data")
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")
            raise

    def prepare_data(self) -> None:
        """Process and combine dataframes"""
        try:
            self.logger.info("Preparing data...")
            
            # Filter by state names
            missing_fips_filtered = self.missing_fips[
                self.missing_fips['Province_State'].isin(self.state_names)
            ].copy()
            
            df_sample_filtered = self.df_sample[
                self.df_sample['Province_State'].isin(self.state_names)
            ].copy()

            # Fill NaNs
            missing_fips_filtered.fillna(0, inplace=True)
            df_sample_filtered.fillna(0, inplace=True)

            # Convert FIPS to numeric, coercing errors to NaN, then drop NaNs
            missing_fips_filtered['FIPS'] = pd.to_numeric(missing_fips_filtered['FIPS'], errors='coerce')
            df_sample_filtered['FIPS'] = pd.to_numeric(df_sample_filtered['FIPS'], errors='coerce')
            
            missing_fips_filtered.dropna(subset=['FIPS'], inplace=True)
            df_sample_filtered.dropna(subset=['FIPS'], inplace=True)

            # Find rows in missing_fips that are not in df_sample
            missing_rows = missing_fips_filtered[~missing_fips_filtered['FIPS'].isin(df_sample_filtered['FIPS'])]
            
            # Concatenate
            self.df_combined = pd.concat([df_sample_filtered, missing_rows], ignore_index=True)
            
            # Remove FIPS=0 (unassigned)
            self.df_combined = self.df_combined[self.df_combined['FIPS'] != 0]
            
            # Format FIPS as 5-digit string for GeoJSON matching
            self.df_combined['FIPS'] = self.df_combined['FIPS'].astype(int).astype(str).str.zfill(5)
            
            # Ensure 'Deaths' column exists (some files use 'Deaths', some 'Death')
            if 'Deaths' in self.df_combined.columns and 'Death' not in self.df_combined.columns:
                self.df_combined.rename(columns={'Deaths': 'Death'}, inplace=True)
            elif 'Death' not in self.df_combined.columns:
                 # Fallback
                 self.df_combined['Death'] = 0

            self.logger.info(f"Prepared {len(self.df_combined)} county records.")
            
        except Exception as e:
            self.logger.error(f"Data preparation failed: {e}")
            raise

    def download_geojson(self) -> None:
        """Download GeoJSON file if it doesn't exist"""
        if not os.path.exists(self.geojson_path):
            self.logger.info(f"GeoJSON file not found. Downloading from {self.geojson_url} to {self.data_dir}...")
            try:
                response = requests.get(self.geojson_url, timeout=10)
                response.raise_for_status()
                with open(self.geojson_path, 'wb') as f:
                    f.write(response.content)
                self.logger.info(f"Successfully downloaded {self.geojson_path}")
            except Exception as e:
                self.logger.error(f"Failed to download GeoJSON: {e}")
                raise

    def create_choropleth(self) -> None:
        """Create the choropleth map visualization"""
        try:
            self.download_geojson()

            self.logger.info(f"Loading GeoJSON file from {self.geojson_path}...")
            with open(self.geojson_path, 'r') as f:
                counties = json.load(f)
            
            # Calculate state totals for hover info
            state_totals = self.df_combined.groupby('Province_State')['Death'].transform('sum')
            self.df_combined['State_Deaths_Total'] = state_totals

            # Create death bins
            # Define bins: 0, 1-5, 6-25, 26-100, 101-500, 500+
            bins = [-1, 0, 5, 25, 100, 500, float('inf')]
            labels = ['0', '1-5', '6-25', '26-100', '101-500', '500+']
            
            self.df_combined['Death_Bin'] = pd.cut(
                self.df_combined['Death'],
                bins=bins,
                labels=labels
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

            self.logger.info("Generating choropleth map...")
            self.fig = px.choropleth(
                self.df_combined,
                geojson=counties,
                locations='FIPS',
                color='Death_Bin',
                scope='usa',
                color_discrete_map=color_map,
                category_orders={'Death_Bin': labels},
                labels={'Death_Bin': 'Deaths'},
                hover_data={
                    'Province_State': True,
                    'Admin2': True,
                    'Death': True,
                    'FIPS': True,
                    'State_Deaths_Total': True,
                    'Death_Bin': False
                }
            )

            # Update traces for better visual
            self.fig.update_traces(
                marker_line_width=0.1,
                marker_line_color='white',
                hovertemplate=(
                    "<b>%{customdata[1]}, %{customdata[0]}</b><br>"
                    "Deaths: %{customdata[2]:,}<br>"
                    "State Total: %{customdata[4]:,}<br>"
                    "<extra></extra>"
                )
            )
            
            # Update layout
            self.fig.update_layout(
                title_text=f'COVID-19 Deaths by County — {self.date}',
                title_x=0.5,
                legend_title_text='Deaths',
                geo=dict(
                    showframe=False,
                    showcoastlines=False,
                    projection_type='albers usa'
                ),
                margin={"r":0,"t":50,"l":0,"b":0},
                template='plotly_dark'
            )
            
        except Exception as e:
            self.logger.error(f"Choropleth creation failed: {e}")
            raise

    def save_figure(self, filename: Optional[str] = None) -> None:
        """Save figure as PNG image"""
        try:
            if filename is None:
                filename = f"covid_choropleth_{self.date}.png"
            
            output_path_png = os.path.join(self.output_dir, filename) # Save PNG to output_dir
            
            self.logger.info(f"Saving image to {output_path_png}...")
            self.fig.write_image(output_path_png, width=1200, height=800, scale=2)
            self.logger.info("Image saved successfully.")
            
            # Also save HTML for interactive view
            html_filename = filename.replace('.png', '.html')
            output_path_html = os.path.join(self.output_dir, html_filename) # Save HTML to output_dir
            self.fig.write_html(output_path_html)
            self.logger.info(f"HTML saved to {output_path_html}")
            
        except Exception as e:
            self.logger.error(f"Image export failed: {e}")
            raise

    def run(self) -> None:
        """Execute complete workflow"""
        self.load_data()
        self.prepare_data()
        self.create_choropleth()
        self.save_figure()

def main():
    parser = argparse.ArgumentParser(description="Generate COVID-19 Choropleth Map")
    parser.add_argument("--date", "-d", default="07-19-2020", help="Date of report (MM-DD-YYYY)")
    parser.add_argument("--output", "-o", default="corona_output", help="Output directory") # Updated default
    parser.add_argument("--data_dir", default="corona_data", help="Directory for input data files like geojson") # New argument
    
    args = parser.parse_args()
    
    try:
        mapper = CoronaMapper(date=args.date, output_dir=args.output, data_dir=args.data_dir)
        mapper.run()
    except Exception as e:
        logging.critical(f"Application failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()