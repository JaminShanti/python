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
pio.renderers.default = "kaleido"

class CoronaMapper:
    """
    A class to generate a choropleth map of COVID-19 deaths by county in the US.
    """
    def __init__(self, date: str = "07-19-2020", output_dir: str = "."):
        self.setup_logger()
        self.date = date
        self.output_dir = output_dir
        
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
        # Note: These URLs are specific to the original script's logic. 
        # Ideally, one would construct the URL dynamically based on self.date, 
        # but the repo structure changed over time. Keeping as is for reproduction of original logic.
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
        self.geojson_path = "geojson-counties-fips.json"

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

            # The logic here seems to be trying to merge FIPS data from an older file (03-30) 
            # where it might be more complete, with the current data.
            # However, the logic in the original script was a bit convoluted.
            # We will try to preserve the intent: ensure we have FIPS for counties.

            # Ensure FIPS are integers (handling potential float conversions)
            # But first, handle non-numeric FIPS if any
            
            # Combine logic from original script:
            # It seems to be trying to find rows in missing_fips that are NOT in df_sample based on FIPS?
            # Or maybe just using missing_fips to fill in gaps?
            # The original script did:
            # self.missing_fips_r = self.missing_fips_r[~self.missing_fips_r.FIPS.isin(self.df_sample_r.FIPS)].dropna()
            # self.df_sample_r = pd.concat([self.df_sample_r, self.missing_fips_r])
            
            # Let's replicate that logic safely
            
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
            self.logger.info(f"GeoJSON file not found. Downloading from {self.geojson_url}...")
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

            self.logger.info("Loading GeoJSON file...")
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
            
            output_path = os.path.join(self.output_dir, filename)
            
            self.logger.info(f"Saving image to {output_path}...")
            self.fig.write_image(output_path, width=1200, height=800, scale=2)
            self.logger.info("Image saved successfully.")
            
            # Also save HTML for interactive view
            html_path = output_path.replace('.png', '.html')
            self.fig.write_html(html_path)
            self.logger.info(f"HTML saved to {html_path}")
            
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
    parser.add_argument("--output", "-o", default=".", help="Output directory")
    
    args = parser.parse_args()
    
    try:
        mapper = CoronaMapper(date=args.date, output_dir=args.output)
        mapper.run()
    except Exception as e:
        logging.critical(f"Application failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
