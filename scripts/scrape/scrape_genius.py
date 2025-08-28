#!/usr/bin/env python3
import argparse, io, os, re, requests, time, unicodedata, yaml
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import List, Optional
from tqdm import tqdm


# Get song lyrics from Genius page
def get_lyrics_data(url_song:str, headers:dict, timeout:int)->str:
    """
    Go to Genius url and return song lyrics as strings
    """

    # Make url request
    res = requests.get(url_song, headers=headers, timeout=timeout)

    # If request successful proceed
    if res.status_code==200:
        
        # Parse and get verses as single string
        soup   = BeautifulSoup(res.text, "html.parser")
        header = "\n".join([i for j in [[line for line in verse.stripped_strings] for verse in soup.find("div", {"data-lyrics-container": "true"}).find("div", {"data-exclude-from-selection": "true"})] for i in j])
        lyrics = "\n".join([i for j in [[line for line in verse.stripped_strings] for verse in soup.find("div", {"data-lyrics-container": "true"})] for i in j])

        # Return lyrics
        return lyrics.replace(header, "")


# Normalize string for querying
def _normalize(s:str)->str:
    """
    Removes accents, lowers and remove extra spaces from string.
    """
    if not s:
        return ""
    
    # Remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    
    # Lowercase, remove extra punctuation/whitespace
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    
    # Return clean string
    return s


# Return index of exact song match
def find_song_index(hits: list, song_name: str, artist_name: str) -> int:
    """
    Looks over Genius search API list of hits and returns if exact match
    """

    # Normalize strings
    target_title = _normalize(song_name)
    target_artist = _normalize(artist_name)
    
    # First pass: try with 'artist_names'
    for i, hit in enumerate(hits):
        if hit["type"]!="song":
            continue
        result = hit["result"]
        artist = _normalize(result["artist_names"])
        title  = _normalize(result["title"])
        
        # Return index of exact match
        if artist==target_artist and title==target_title:
            return i
    
    # Second pass: fallback to 'primary_artist_names'
    for i, hit in enumerate(hits):
        if hit["type"]!="song":
            continue
        result = hit["result"]
        artist = _normalize(result["primary_artist_names"])
        title  = _normalize(result["title"])
        
        # Return index of exact match
        if artist==target_artist and title==target_title:
            return i

    # Return None if no exact match
    return -1


# Main function to use Genius to collect song information
def main():
    """
    Uses Genius API and urls to find song data and lyrics.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Uses Genius API and url to collect song lyrics.")
    ap.add_argument("--scrape_config", default="configs/scrape.yaml", help="Path to YAML with scraping configurations")
    ap.add_argument("--output_name", default="songs", help="Output filename")
    args = ap.parse_args()

    # Load scraping parameters
    with open(args.scrape_config, "r") as f:
        scrape_config = yaml.safe_load(f)

    # Get key scraping parameters
    base_url    = scrape_config["params"]["api_search_api_url"]
    headers     = {"User-Agent": scrape_config["params"]["header"]}
    output_name = f"{args.output_name}.parquet"
    output_path = f"./data/songs/{output_name}"

    # Load environmental variables and Genius API key secret
    load_dotenv()
    GENIUS_API_KEY = os.getenv("GENIUS_API_KEY")
    
    # Verify if initial song is available
    if os.path.exists(output_path):
        df_songs = pd.read_parquet(output_path)
    
    # Otherwise create from Billboard data
    else:
        df_songs = pd.read_parquet("./data/raw/billboard.parquet")
        df_songs = df_songs[["artist", "song"]].drop_duplicates().reset_index(drop=True)

        # Create relevant columns
        for v in ["genius_artist", "genius_song", "genius_url", "lyrics"]:
            df_songs[v] = None

    # Go over rows to collect data from Genius search API
    for i in tqdm(df_songs.sample(400).query("genius_url!=genius_url").index, desc="Genius API search"):

        # Get song and artist names
        song_name   = df_songs.loc[i, "song"]
        artist_name = df_songs.loc[i, "artist"]

        # Make Genius search API request for song
        url_api  = base_url.format(ACCESS_TOKEN=GENIUS_API_KEY, QUERY=song_name)
        res      = requests.get(url_api, headers=headers, timeout=30)
        res_dict = res.json()

        # Find exact hit
        idx_hit = find_song_index(res_dict["response"]["hits"], song_name, artist_name)

        # Populate if exact hit
        if idx_hit!=-1:
            df_songs.loc[i, "genius_artist"] = res_dict["response"]["hits"][idx_hit]["result"]["artist_names"]
            df_songs.loc[i, "genius_song"]   = res_dict["response"]["hits"][idx_hit]["result"]["title"]
            df_songs.loc[i, "genius_url"]    = res_dict["response"]["hits"][idx_hit]["result"]["url"]

        # If no exact match try variant
        else:
            # Make Genius search API request for song
            url_api  = base_url.format(ACCESS_TOKEN=GENIUS_API_KEY, QUERY=f"{song_name} by {artist_name}")
            res      = requests.get(url_api, headers=headers, timeout=30)
            res_dict = res.json()

            # Find exact hit
            idx_hit = find_song_index(res_dict["response"]["hits"], song_name, artist_name)

            # Populate if exact hit
            if idx_hit!=-1:
                df_songs.loc[i, "genius_artist"] = res_dict["response"]["hits"][idx_hit]["result"]["artist_names"]
                df_songs.loc[i, "genius_song"]   = res_dict["response"]["hits"][idx_hit]["result"]["title"]
                df_songs.loc[i, "genius_url"]    = res_dict["response"]["hits"][idx_hit]["result"]["url"]

    # Get lyrics based on Genius url
    for i in tqdm(df_songs.query("genius_url==genius_url and lyrics!=lyrics").index, desc="Scraping Genius lyrics"):
        
        # Get song url
        url_song = df_songs.loc[i, "genius_url"]

        # Get song
        song_lyrics = get_lyrics_data(url_song, headers, 30)

        # Populate if song
        if song_lyrics:
            df_songs.loc[i, "lyrics"] = song_lyrics

    
    # Save to data folder locally
    df_songs.to_parquet(output_path, index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()
