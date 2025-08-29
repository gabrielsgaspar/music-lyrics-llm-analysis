#!/usr/bin/env python3
import argparse, io, json, os, re, requests, time, unicodedata, yaml
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from transformers import logging
from typing import List, Optional
from tqdm import tqdm

# Disable loading messages for this script
logging.set_verbosity_error()


# Create embeddings dictionary from model
def make_lyrics_embeddings(model:SentenceTransformer, summaries:list, song_ids:list, batch_size=int, normalize_embeddings:bool=True)->dict:
    """
    Uses model in argument to create embeddings for strings.
    """

    # Retrieve embedding and convert to dictionary
    embeddings = model.encode(summaries, batch_size=batch_size, show_progress_bar=False, normalize_embeddings=normalize_embeddings)
    embeddings = {song_id: embedding.astype(float).tolist() for song_id, embedding in zip(song_ids, np.asarray(embeddings))}

    # Return embeddings dictionary
    return embeddings


# Main function to use create embeddings for song lyrics
def main():
    """
    Uses model to create lyric embeddings.
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Uses sentence transformer to create embeddings.")
    ap.add_argument("--embedding_model", default="all-mpnet-base-v2", help="Name of sentence transformer model")
    ap.add_argument("--batch_size", default=20, help="Number of summaries in batch")
    ap.add_argument("--songs_path", default="./data/songs/songs.parquet", help="Path to songs data")
    ap.add_argument("--output_name", default="embeddings", help="Output filename")
    args = ap.parse_args()

    # Load sentence transformer
    model_name = args.embedding_model
    model      = SentenceTransformer(model_name)

    # Get key parameters and paths
    batch_size  = args.batch_size
    output_name = f"{args.output_name}.json"
    output_path = f"./data/songs/{output_name}"
    songs_path  = args.songs_path

    # Load songs data
    df_songs = pd.read_parquet(songs_path)

    # Check if json with embeddings exits
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            embeddings_dict = json.load(f)
    
    # Otherwise create it
    else:
        embeddings_dict = {}
        with open(output_path, "w") as f:
            json.dump(embeddings_dict, f, indent=2)

    # Get list with processed ids
    processed_ids = list(embeddings_dict.keys())

    # Keep only rows with a summary and skip ones already embedded
    work_df = df_songs.query("summary==summary").copy()
    work_df = work_df.query(f"genius_song_id not in @processed_ids")

    # Go over chunkcs
    for start in tqdm(range(0, len(work_df), batch_size), desc="Getting embeddings"):

        # Get chunk, summaries and ids
        chunk     = work_df.iloc[start:start + batch_size]
        summaries = chunk["summary"].astype(str).str.strip().tolist()
        song_ids  = chunk["genius_song_id"].astype(str).tolist()

        # Get dictionary with embeddings
        batch_embeddings = make_lyrics_embeddings(model                = model,
                                                  summaries            = summaries,
                                                  song_ids             = song_ids,
                                                  batch_size           = batch_size,
                                                  normalize_embeddings = True)

        # Extend the running dictionary
        embeddings_dict.update(batch_embeddings)

    # Save to data folder locally
    with open(output_path, "w") as f:
        json.dump(embeddings_dict, f, indent=2)


# Run script directly
if __name__ == "__main__":
    main()
