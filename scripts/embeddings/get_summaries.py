#!/usr/bin/env python3
import argparse, io, json, os, re, requests, time, unicodedata, yaml
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from typing import List, Optional
from tqdm import tqdm


# Use OpenAI API to get summary of lyrics
def get_lyrics_data(lyrics:str, client:OpenAI, params:dict)->dict:
    """
    Produces dictionary with lyrics summary
    """

    # Organize system and user messages
    system_message = {"role": "system", "content": params["prompts"]["system"]}
    user_message   = {"role": "user", "content": params["prompts"]["user"].format(LYRICS_TEXT=lyrics)}
    messages       = [system_message, user_message]

    # Make API request to get json and turn into dictionary
    response = client.chat.completions.create(model           = params["model"],
                                              response_format = {"type": "json_object"},
                                              messages        = messages,
                                              temperature     = params["temperature"],
                                              seed            = params["seed"])
    res_dict = json.loads(response.choices[0].message.content)

    # Return dictionary
    return res_dict
        

# Get lyrics summaries using OpenAI ChatGPT
def main():
    """
    Main function to call script to get summaries.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Uses Genius API and url to collect song lyrics.")
    ap.add_argument("--prompts", default="prompts/summarize.yaml", help="Path to YAML with summarization prompts and meta data")
    ap.add_argument("--output_name", default="songs", help="Output filename")
    args = ap.parse_args()

    # Load prompts
    with open(args.prompts, "r") as f:
        prompts = yaml.safe_load(f)

    # Organize parameters dictionary for function
    params = {"prompts"    : {"system": prompts["system"], "user": prompts["user"]},
              "model"      : prompts["model"]["name"],
              "temperature": prompts["model"]["temperature"],
              "seed"       : prompts["model"]["seed"]}
    
    # Set output paths
    output_name = f"{args.output_name}.parquet"
    output_path = f"./data/songs/{output_name}"

    # Load environmental variables and OpenAI API key secret
    load_dotenv()
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # Initiate client to make calls
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Load songs file
    df_songs = pd.read_parquet(output_path)

    # Go over rows and get summaries
    for i in tqdm(df_songs.query("lyrics==lyrics and lyrics not in ['Instrumental', 'Lyrics not available']").index, desc="Getting summaires"):

        # Get lyrics and summary
        lyrics  = df_songs.loc[i, "lyrics"]
        summary = get_lyrics_data(lyrics, client, params)

        # Populate row
        df_songs.loc[i, "summary"] = summary["summary"]
        
    # Save to data folder locally
    df_songs.to_parquet(output_path, index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()
