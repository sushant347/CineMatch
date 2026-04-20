"""
Data Preparation Pipeline
=========================
Cleans, merges, and subsamples MovieLens 20M + TMDB datasets.
Outputs: final_movies.csv, ratings_subset.csv, movies_merged.csv
"""

import os
import re
import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
OUT_DIR = os.path.join(BASE_DIR, "data", "processed")
os.makedirs(OUT_DIR, exist_ok=True)

RATINGS_SUBSAMPLE = 300_000  # enough signal for collaborative filtering quality
MIN_MOVIE_YEAR = 2000


def extract_year(title: str):
    """Extract year from MovieLens title like 'Toy Story (1995)'."""
    match = re.search(r"\((\d{4})\)\s*$", str(title))
    return int(match.group(1)) if match else None


def clean_title(title: str) -> str:
    """Normalize title for matching: lowercase, strip, remove year suffix."""
    t = str(title).strip().lower()
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)  # remove trailing (year)
    t = re.sub(r"[^\w\s]", "", t)  # remove punctuation
    t = re.sub(r"\s+", " ", t).strip()
    return t


def main():
    print("=" * 60)
    print("PHASE 1: DATA PREPARATION")
    print("=" * 60)

    # ── 1. Load raw datasets ──────────────────────────────────────
    print("\n[1/6] Loading datasets...")

    movies_ml = pd.read_csv(os.path.join(RAW_DIR, "movies.csv"))
    print(f"  MovieLens movies: {len(movies_ml):,} rows")

    links = pd.read_csv(os.path.join(RAW_DIR, "links.csv"))
    print(f"  Links: {len(links):,} rows")

    tmdb = pd.read_csv(os.path.join(RAW_DIR, "TMDB_movie_dataset_v11.csv"))
    print(f"  TMDB movies: {len(tmdb):,} rows")

    # Load ratings in chunks for memory efficiency
    print(f"  Loading ratings (subsampling to {RATINGS_SUBSAMPLE:,})...")
    ratings_full = pd.read_csv(os.path.join(RAW_DIR, "ratings.csv"))
    print(f"  Full ratings: {len(ratings_full):,} rows")

    # ── 2. Clean MovieLens movies ─────────────────────────────────
    print("\n[2/6] Cleaning MovieLens movies...")
    movies_ml["year"] = movies_ml["title"].apply(extract_year)
    movies_ml["clean_title"] = movies_ml["title"].apply(clean_title)
    movies_ml = movies_ml.dropna(subset=["year"])
    movies_ml["year"] = movies_ml["year"].astype(int)
    movies_ml = movies_ml[movies_ml["year"] >= MIN_MOVIE_YEAR]
    print(f"  After cleaning: {len(movies_ml):,} movies with year info")

    # ── 3. Clean TMDB dataset ────────────────────────────────────
    print("\n[3/6] Cleaning TMDB dataset...")
    tmdb = tmdb.dropna(subset=["id", "title"])
    tmdb["id"] = tmdb["id"].astype(int)

    # Fill missing text fields
    tmdb["overview"] = tmdb["overview"].fillna("")
    tmdb["genres"] = tmdb["genres"].fillna("")
    tmdb["keywords"] = tmdb["keywords"].fillna("")
    tmdb["original_language"] = tmdb["original_language"].fillna("en")
    tmdb["popularity"] = pd.to_numeric(tmdb["popularity"], errors="coerce").fillna(0)
    tmdb["vote_average"] = pd.to_numeric(tmdb["vote_average"], errors="coerce").fillna(0)
    tmdb["poster_path"] = tmdb["poster_path"].fillna("")
    tmdb["backdrop_path"] = tmdb["backdrop_path"].fillna("")
    tmdb["release_date"] = tmdb["release_date"].fillna("")
    tmdb["tagline"] = tmdb["tagline"].fillna("")

    # Keep only released movies
    tmdb = tmdb[tmdb["status"] == "Released"]
    print(f"  After cleaning: {len(tmdb):,} released TMDB movies")

    # ── 4. Merge via links.csv ───────────────────────────────────
    print("\n[4/6] Merging datasets via links.csv (movieId → tmdbId)...")
    links = links.dropna(subset=["tmdbId"])
    links["tmdbId"] = links["tmdbId"].astype(int)

    # Step 1: MovieLens movies + links → get tmdbId
    movies_with_tmdb = movies_ml.merge(links[["movieId", "tmdbId"]], on="movieId", how="inner")
    print(f"  MovieLens movies with tmdbId: {len(movies_with_tmdb):,}")

    # Step 2: Join with TMDB metadata
    movies_merged = movies_with_tmdb.merge(
        tmdb[["id", "overview", "original_language", "popularity", "vote_average",
              "poster_path", "backdrop_path", "genres", "keywords", "release_date",
              "tagline", "runtime"]].rename(columns={"id": "tmdbId"}),
        on="tmdbId",
        how="inner",
        suffixes=("_ml", "_tmdb")
    )

    # Rename for clarity
    movies_merged = movies_merged.rename(columns={
        "genres_tmdb": "genres_detailed",
        "genres_ml": "genres",
    }) if "genres_ml" in movies_merged.columns else movies_merged.rename(columns={
        "genres_x": "genres",
        "genres_y": "genres_detailed",
    }) if "genres_x" in movies_merged.columns else movies_merged

    print(f"  Merged movies: {len(movies_merged):,}")

    # ── 5. Subsample ratings ─────────────────────────────────────
    print("\n[5/6] Subsampling ratings...")

    # Keep only ratings for movies that exist in our merged set
    valid_movie_ids = set(movies_merged["movieId"].unique())
    ratings_valid = ratings_full[ratings_full["movieId"].isin(valid_movie_ids)]
    print(f"  Ratings for merged movies: {len(ratings_valid):,}")

    if len(ratings_valid) > RATINGS_SUBSAMPLE:
        # Stratified subsample: keep diverse users
        ratings_subset = ratings_valid.sample(n=RATINGS_SUBSAMPLE, random_state=42)
    else:
        ratings_subset = ratings_valid
    print(f"  Final ratings subset: {len(ratings_subset):,}")

    # ── 6. Save processed files ──────────────────────────────────
    print("\n[6/6] Saving processed files...")

    # Movies metadata
    movies_out = movies_merged[["movieId", "tmdbId", "title", "year", "genres",
                                 "overview", "original_language", "popularity",
                                 "vote_average", "poster_path", "backdrop_path",
                                 "keywords", "release_date", "tagline"]].copy()
    movies_out = movies_out.drop_duplicates(subset=["movieId"])
    movies_out["year"] = pd.to_numeric(movies_out["year"], errors="coerce")
    movies_out = movies_out[movies_out["year"] >= MIN_MOVIE_YEAR]
    movies_out["year"] = movies_out["year"].astype(int)

    movies_out["poster_path"] = movies_out["poster_path"].astype(str).str.strip()
    movies_out["backdrop_path"] = movies_out["backdrop_path"].astype(str).str.strip()
    before_image_filter = len(movies_out)
    movies_out = movies_out[
        (movies_out["poster_path"] != "")
        | (movies_out["backdrop_path"] != "")
    ]
    removed_without_images = before_image_filter - len(movies_out)
    if removed_without_images > 0:
        print(f"  Removed {removed_without_images:,} movies without images")

    movies_out.to_csv(os.path.join(OUT_DIR, "movies_merged.csv"), index=False)
    print(f"  ✅ movies_merged.csv: {len(movies_out):,} movies")

    # Ratings
    ratings_subset.to_csv(os.path.join(OUT_DIR, "ratings_subset.csv"), index=False)
    print(f"  ✅ ratings_subset.csv: {len(ratings_subset):,} ratings")

    # Final combined (for quick reference)
    final = ratings_subset.merge(movies_out, on="movieId", how="inner")
    final.to_csv(os.path.join(OUT_DIR, "final_movies.csv"), index=False)
    print(f"  ✅ final_movies.csv: {len(final):,} rows")

    # Stats
    print("\n" + "=" * 60)
    print("DATA PREPARATION COMPLETE")
    print("=" * 60)
    print(f"  Total unique movies: {movies_out['movieId'].nunique():,}")
    print(f"  Total unique users: {ratings_subset['userId'].nunique():,}")
    print(f"  Total ratings: {len(ratings_subset):,}")
    print(f"  Languages: {movies_out['original_language'].value_counts().head(5).to_dict()}")
    hindi_count = len(movies_out[movies_out["original_language"] == "hi"])
    print(f"  Hindi movies: {hindi_count}")
    print(f"\n  Output directory: {OUT_DIR}")


if __name__ == "__main__":
    main()

