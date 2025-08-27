#!/usr/bin/env python3
import argparse, io, os, re, requests, time, yaml
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Optional
from tqdm import tqdm


# Get artist and album data from url
def get_artists_albums(url:str, headers:dict, timeout:int, release_year:int)->dict:
    """
    Checks if page has artists and albums and returns dictionary with data.
    """

    # Initiate dictionary to hold data
    page_data = {"artist": [], "album": [], "release_year": []}

    # Request url
    res = requests.get(url, headers=headers, timeout=timeout)

    # Check if successfull request
    if res.status_code==200:

        # Parse with beaufitulsoup
        soup = BeautifulSoup(res.text, "html.parser")

        # Retrieve table
        table = soup.find("table", {"id": "search-award-table"})

        # Proceed if table exists
        if table:

            # Get row with artists data
            rows  = table.find_all("tr", {"class": "table_award_row"})

            # Get artists, album and release_year data
            for row in rows:
                artist = row.find("td", {"class": "artists_cell"}).text.lower().strip()
                album  = row.find_all("td", {"class": "others_cell"})[0].text.lower().strip()
                
                # Append to dictionary
                page_data["artist"].append(artist)
                page_data["album"].append(album)
                page_data["release_year"].append(release_year)

        # Return dictionary with results
        return page_data
    
    # If error return dictinonary with error variables
    else:
        return {"error": res.status_code}


# Main function to scrape RIAA data
def main():
    """
    Main function to scrape all relevant albums and artist data from RIAA website.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Scrape RIAA data for algums with Gold or Platinum status and save locally.")
    ap.add_argument("--scrape_config", default="configs/riaa/scrape.yaml", help="Path to YAML with scraping configurations for RIAA website")
    ap.add_argument("--output_name", default="riaa_data", help="Output filename")
    args = ap.parse_args()

    # Load RIAA scraping parameters
    with open(args.scrape_config, "r") as f:
        scrape_config = yaml.safe_load(f)

    # Get key scraping parameters
    base_url     = scrape_config["params"]["base_url"]
    headers      = {"User-Agent": scrape_config["params"]["header"]}
    start_year   = scrape_config["params"]["start_year"]
    end_year     = scrape_config["params"]["end_year"]
    month_ranges = [(i, i+1) for i in range(1, 12, 2)]
    output_name  = f"{args.output_name}.parquet"

    # Initiate dictionary to hold data
    riaa_data = {"artist": [], "album": [], "release_year": []}

    # Go over years and months
    for year in tqdm(range(start_year, end_year + 1), desc="Scraping RIAA data"):
        for month_range in month_ranges:
            
            # Define bound months
            from_time = f"{year}-{'{:02d}'.format(month_range[0])}-01"
            to_time   = f"{year}-{'{:02d}'.format(month_range[1])}-31"

            # Get url for period
            url = base_url.format(FROM=from_time, TO=to_time)

            # Retrieve data
            page_data = get_artists_albums(url, headers, 30, year)

            # Proceed if not error
            if "error" not in page_data.keys():
            
                # Extend main data dictionary if not empty
                if not all(isinstance(v, list) and not v for v in page_data.values()):
                    for k in page_data.keys():
                        riaa_data[k] = riaa_data[k] + page_data[k]
                    
            else:
                print(f"Error in page {url}")
                time.sleep(30)

        # Sleep after year
        time.sleep(10)

    # Create dataframe from scraped data
    df = pd.DataFrame(riaa_data)
    
    # Save to data folder locally
    df.to_parquet(f"./data/riaa/{output_name}", index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()