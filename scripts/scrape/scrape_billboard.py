#!/usr/bin/env python3
import argparse, io, os, re, requests, time, yaml
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Optional
from tqdm import tqdm


# Get weekly top songs from Billboard
def get_weekly_data(url:str, headers:dict, timeout:int, chart_week:str)->dict:
    """
    Scrapes weekly chart of top 100 songs.
    """

    # Initiate dictionary to hold data
    chart_data = {"artist": [], "song": [], "chart_week": [], "position": []}

    # Request url
    res = requests.get(url, headers=headers, timeout=timeout)

    # Check if successfull request
    if res.status_code==200:

        # Parse with beaufitulsoup
        soup = BeautifulSoup(res.text, "html.parser")

        # Retrieve table chart
        chart = soup.find("div", {"class": "chart-results-list"})

        # Proceed if chart table exists
        if chart:

            # Get row with artists data
            rows = chart.find_all("div", {"class": "o-chart-results-list-row-container"})

            # Iterate over chart rows
            for i, row in enumerate(rows):

                # Retrieve names
                try:
                    artist = row.find_all("li", {"class": "o-chart-results-list__item"})[3].find("span").text.strip()
                    song   = row.find_all("li", {"class": "o-chart-results-list__item"})[3].find("h3").text.strip()
                except:
                    artist = row.find_all("li", {"class": "o-chart-results-list__item"})[2].find("span").text.strip()
                    song   = row.find_all("li", {"class": "o-chart-results-list__item"})[2].find("h3").text.strip()
              
                # Append to dictionary
                chart_data["artist"].append(artist)
                chart_data["song"].append(song)
                chart_data["chart_week"].append(chart_week)
                chart_data["position"].append(i+1)

        # Return dictionary with results
        return chart_data
    
    # If error return dictinonary with error variables
    else:
        return {"error": res.status_code}


# Main function to scrape Billboard hot 100 data
def main():
    """
    Main function to scrape top songs from Billboard website.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Scrape Billboard hot-100 data and save locally.")
    ap.add_argument("--scrape_config", default="configs/scrape.yaml", help="Path to YAML with scraping configurations")
    ap.add_argument("--output_name", default="billboard2", help="Output filename")
    args = ap.parse_args()

    # Load scraping parameters
    with open(args.scrape_config, "r") as f:
        scrape_config = yaml.safe_load(f)

    # Get key scraping parameters
    base_url    = scrape_config["params"]["billboard_base_url"]
    headers     = {"User-Agent": scrape_config["params"]["header"]}
    start_year  = scrape_config["params"]["start_year"]
    end_year    = scrape_config["params"]["end_year"]
    output_name = f"{args.output_name}.parquet"
    output_path = f"./data/raw/{output_name}"

    # Create list with weeks, starting in August (first time published)
    date_range = pd.date_range(start=f"{start_year}-08-01", end=f"{end_year}-12-31", freq="D")
    chart_weeks = date_range[date_range.dayofweek==6]
    chart_weeks = [w.strftime('%Y-%m-%d') for w in chart_weeks]
    chart_weeks = ['1986-08-31', '1999-05-16']
    
    # Initiate dictionary to hold data
    billboard_data = {"artist": [], "song": [], "chart_week": [], "position": []}

    # Go over weeks
    for chart_week in tqdm(chart_weeks, desc="Scraping Billboard data"):
            
        # Set url and get data
        week_url   = f"https://www.billboard.com/charts/hot-100/{chart_week}/"

        # Try to get data
        try:
            chart_data = get_weekly_data(week_url, headers, 30, chart_week)

            # Proceed if not error
            if "error" not in chart_data.keys():
                for k in chart_data.keys():
                        billboard_data[k] = billboard_data[k] + chart_data[k]

            else:
                print(f"Error in page {week_url}")
                time.sleep(30)
        
        # If exception print url
        except:
            print(f"Error in page {week_url}")
            time.sleep(30)

    # Create dataframe from scraped data
    df = pd.DataFrame(billboard_data)
    
    # Save to data folder locally
    df.to_parquet(output_path, index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()