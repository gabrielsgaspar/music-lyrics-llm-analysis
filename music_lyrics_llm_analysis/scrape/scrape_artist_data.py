#!/usr/bin/env python3
import argparse, io, json, os, re, requests, time, yaml
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from openai import OpenAI
from rapidfuzz import fuzz, process
from google.cloud import storage
from urllib.parse import quote, quote_plus
from typing import List, Optional
from tqdm import tqdm


# Function to expand dataframe based on mapping
def expand_with_dict(df: pd.DataFrame, artist_col: str, mapping: dict)->pd.DataFrame:
    """
    Expand pandas dataframe according to mapping to artists.
    """
    # Turn each row into a list of artists (either from dict or [original])
    lists = df[artist_col].map(mapping).where(lambda s: s.notna(), other=df[artist_col].apply(lambda x: [x]))
    
    # Copy df but drop the original artist_col
    out = df.drop(columns=[artist_col]).copy()
    out["_artist_list"] = lists
    
    # Explode
    out = out.explode("_artist_list", ignore_index=True)
    
    # Rename exploded col back to artist_col
    out = out.rename(columns={"_artist_list": artist_col})

    # Return dataframe
    return out


# Function to search MusicBrainz API in search for artist
def find_musicbrainz_url(artist_name:str, headers:dict, timeout:int)->dict:
    """
    Uses MusicBrainz API to find artist url.
    """

    # Encode artist string and set url
    artist_str = quote(artist_name)
    search_url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{artist_str}&fmt=json"

    # Make request
    res = requests.get(search_url, headers=headers, timeout=30)

    # Proceed if success
    if res.status_code==200:
        
        # Get dictionary and list with matches
        res_dict = res.json()
        matches  = res_dict["artists"]

        # Check if there is 100 score match first
        if matches[0]["score"]==100:
            idx = 0
        
        # Otherwise find index of top fuzzy match
        else:
            _, _, idx = process.extractOne(artist_name, [i["name"] for i in matches], scorer=fuzz.token_sort_ratio, processor=lambda x: x.lower())
        
        # Create dictionary with data
        query_dict = {"type"          : matches[idx]["type"].lower().strip() if "type" in matches[idx].keys() else None,
                      "gender"        : matches[idx]["gender"].lower().strip() if "gender" in matches[idx].keys() else None,
                      "country"       : matches[idx]["country"].lower().strip() if "country" in matches[idx].keys() else None,
                      "time_begin"    : matches[idx]["life-span"]["begin"] if "begin" in matches[idx]["life-span"].keys() else None,
                      "time_end"      : matches[idx]["life-span"]["end"] if "end" in matches[idx]["life-span"].keys() else None,
                      "begin_area_id" : matches[idx]["begin-area"]["id"] if "begin-area" in matches[idx].keys() else None,
                      "end_area_id"   : matches[idx]["end-area"]["id"] if "end-area" in matches[idx].keys() else None,
                      "mb_id"         : matches[idx]["id"].lower().strip()}

        # Return query dictionary
        return query_dict


# Scrape additional information for artists
def get_artist_details(mb_id:str, headers:dict, timeout:int)->dict:
    """
    Go to MusicBrainz url and scrape additional information.
    """

    # Set url, make request and read with BeautifulSoup
    url  = f"https://musicbrainz.org/artist/{mb_id}/relationships"
    res  = requests.get(url, headers=headers, timeout=timeout)
    soup = BeautifulSoup(res.text, "html.parser")

    # Find tables with details class
    tables = soup.find_all("table", {"class": "details"})

    # Find wikidata, discogs and genius favicons in list
    wikidata_li = soup.find("li", {"class": "wikidata-favicon"})
    discogs_li  = soup.find("li", {"class": "discogs-favicon"})
    genius_li   = soup.find("li", {"class": "genius-favicon"})

    # Get wikidata and discogs id if it exists, else null
    wikidata_id = wikidata_li.find("a").text.split(":")[-1].strip() if wikidata_li else ""
    discogs_id  = discogs_li.find("a")["href"].split("/")[-1] if discogs_li else ""
    genius_id   = ""

    # Get genius url if it exists, else null
    genius_url  = genius_li.find("a")["href"] if genius_li else None

    # Initiate dictionary to hold extra data
    info_dict = {}

    # Put values in dictionary
    data_dict = {"wikidata_id" : wikidata_id,
                 "discogs_id"  : discogs_id,
                 "genius_url"  : genius_url}

    # Return data dictionary
    return data_dict


# Main function to get additional artist data
def main():
    """
    Main function to scrape for more artist data.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Scrape RIAA data for algums with Gold or Platinum status and save locally.")
    ap.add_argument("--prompt_config", default="prompts/cleaning/riaa_artists.yaml", help="Path to YAML with scraping promts")
    ap.add_argument("--scrape_config", default="configs/riaa/scrape.yaml", help="Path to YAML with scraping configurations for RIAA website")
    ap.add_argument("--output_path", default="./output/scrape", help="Output path for prompt responses")
    ap.add_argument("--override_prompt", default=False, help="Boolean to force prompt if file not available")
    ap.add_argument("--override_data", default=False, help="Boolean to force organizing from scratch")
    ap.add_argument("--riaa_data", default="./data/riaa/riaa_data.parquet", help="Name of RIAA scraped file")
    ap.add_argument("--artist_path", default="./data/artists/artists.parquet", help="Output filename")
    args = ap.parse_args()

    # Load scraping parameters
    with open(args.scrape_config, "r") as f:
        scrape_config = yaml.safe_load(f)

    # Scraping parameters
    headers = {"User-Agent": scrape_config["params"]["header"]}
    timeout = 30

    # Load path variables
    output_path = args.output_path
    artist_path = args.artist_path

    # Check if need to load data or not
    if args.override_data:
        
        # Load scraped RIAA data
        df = pd.read_parquet(args["riaa_data"])

        # Organize unique artists
        df_artists = df.query("artist not in ['various', 'soundtrack']")[["artist"]].drop_duplicates().copy()

        # Get list of possible extension artists (e.g. Marvin Gaye, Tammi Terrel should be two entries)
        artist_ext_list = df_artists.query("artist.str.contains('&') or artist.str.contains(' and ')  or artist.str.contains(' y ') or artist.str.contains(',')").artist.unique()

        # Load environmental variables and initiate OpenAI client
        load_dotenv()
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        client         = OpenAI(api_key=OPENAI_API_KEY)

        # Load RIAA prompts
        with open(args.prompt_config, "r") as f:
            riaa_prompts = yaml.safe_load(f)

        # Define prompt dictionaries based on yaml and other parameters
        system_message = {"role": "system", "content": (riaa_prompts["system"])}
        user_message   = {"role": "user", "content": (riaa_prompts["user"].format(artist_list=artist_ext_list))}
        model          = riaa_prompts["model"]["name"]
        temperature    = riaa_prompts["model"]["temperature"]
        seed           = riaa_prompts["model"]["seed"]

        # Force prompt if boolean or path to prompt results not exists
        if args["override_prompt"] or not os.path.exists(f"{output_path}/gpt_artist_extended.json"):
        
            # Define overall message
            messages = [system_message, user_message]

            # Make API call
            response = client.chat.completions.create(model         = model,
                                                    response_format = {"type": "json_object"},
                                                    messages        = messages,
                                                    temperature     = temperature,
                                                    seed            = seed)

            # Get dictionary with responses and save
            chat_dict = json.loads(response.choices[0].message.content)
            with open(f"{output_path}/gpt_artist_extended.json", "w", encoding="utf-8") as f:
                json.dump(chat_dict, f, ensure_ascii=False, indent=2)

        # Otherwise load prompt responses
        else:
            with open(f"{output_path}/gpt_artist_extended.json", "r", encoding="utf-8") as f:
                chat_dict = json.load(f)

        # Filter for keys with more than one item
        chat_dict = {key: items for key, items in chat_dict.items() if len(items)>1}

        # Expand artists dataframe
        df_artists = expand_with_dict(df_artists, "artist", chat_dict)

        # Start relevant variables
        for v in ["type", "gender", "country", "time_begin", "time_end", "begin_area_id", "end_area_id", "mb_id", "wikidata_id", "discogs_id", "genius_url"]:
            df_artists[v] = None

    # Otherwise load (partially) clean data
    else:
        
        df_artists = pd.read_parquet(f"{artist_path}")

    # Go over rows to populate data
    for r in tqdm(df_artists.query("mb_id!=mb_id").index, desc="Querying MusicBrainz API"):

        # Attempt to get data
        try:
            # Retrieve data
            mb_data = find_musicbrainz_url(df_artists.loc[r, "artist"], headers, 30)

            # Check if returned
            if mb_data:
                # Update dataframe
                for v in mb_data.keys():
                    df_artists.loc[r, v] = mb_data[v]  
                
            # Otherwise sleep to give API a break
            else:
                time.sleep(30)
        
        # Print if exception
        except:
            print(df_artists.loc[r, "artist"])
            time.sleep(10)
     
    # Go over artists to retrieve additional information
    for r in tqdm(df_artists.query("genius_url!=genius_url and mb_id==mb_id").index, desc="Getting additional data"):

        # Attempt to get data
        try:
            # Retrieve additional data
            info_dict = get_artist_details(df_artists.loc[r, "mb_id"], headers, 30)

            # Update dataframe
            for v in info_dict.keys():
                df_artists.loc[r, v] = info_dict[v]  

        
        # Print if exception
        except:
            print(df_artists.loc[r, "artist"])
            time.sleep(10)

    # Save artist data
    df_artists.to_parquet(f"{artist_path}", index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()