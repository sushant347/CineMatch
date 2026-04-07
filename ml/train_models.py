"""
ML Model Training Pipeline
===========================
Trains content-based (TF-IDF), collaborative (ALS), and hybrid models.
Saves all artifacts to ml/models/

Uses implicit ALS on sparse matrices for collaborative filtering.
"""

import os
import pickle
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler
from implicit.als import AlternatingLeastSquares

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
MODEL_DIR = os.path.join(BASE_DIR, "ml", "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# Limit users for memory — keep top N most active users
MAX_USERS = 20000
N_ALS_FACTORS = 64
ALS_ALPHA = 20.0
ALS_ITERATIONS = 25
ALS_REGULARIZATION = 0.05


def build_content_model(movies: pd.DataFrame):
    """Build TF-IDF content-based model on overview + genres + keywords."""
    print("\n── Content-Based Model (TF-IDF) ──")

    movies = movies.copy()
    movies["combined_text"] = (
        movies["overview"].fillna("") + " " +
        movies["genres"].fillna("").str.replace("|", " ", regex=False) + " " +
        movies["keywords"].fillna("")
    )

    # Filter out movies with no text
    movies = movies[movies["combined_text"].str.strip().str.len() > 5].copy()
    movies = movies.reset_index(drop=True)

    print(f"  Building TF-IDF on {len(movies):,} movies...")
    tfidf = TfidfVectorizer(
        max_features=8000,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95
    )
    tfidf_matrix = tfidf.fit_transform(movies["combined_text"])
    print(f"  TF-IDF matrix shape: {tfidf_matrix.shape}")

    # Create movie index mapping (movieId → row index in tfidf_matrix)
    movie_indices = pd.Series(range(len(movies)), index=movies["movieId"])

    # Save
    with open(os.path.join(MODEL_DIR, "tfidf_vectorizer.pkl"), "wb") as f:
        pickle.dump(tfidf, f)
    with open(os.path.join(MODEL_DIR, "tfidf_matrix.pkl"), "wb") as f:
        pickle.dump(tfidf_matrix, f)
    with open(os.path.join(MODEL_DIR, "movie_indices.pkl"), "wb") as f:
        pickle.dump(movie_indices, f)

    print("  ✅ Content model saved")
    return tfidf_matrix, movie_indices, movies


def build_collaborative_model(ratings: pd.DataFrame, movies: pd.DataFrame):
    """Build ALS collaborative filtering model using sparse matrices."""
    print("\n── Collaborative Filtering Model (ALS) ──")

    # Filter ratings to only include movies in our set
    valid_movies = set(movies["movieId"].unique())
    ratings = ratings[ratings["movieId"].isin(valid_movies)].copy()

    # Keep top N most active users to control memory
    user_counts = ratings["userId"].value_counts()
    top_users = set(user_counts.head(MAX_USERS).index)
    ratings = ratings[ratings["userId"].isin(top_users)].copy()

    # Create mappings
    user_ids = sorted(ratings["userId"].unique())
    movie_ids = sorted(ratings["movieId"].unique())

    user_to_idx = {uid: i for i, uid in enumerate(user_ids)}
    movie_to_idx = {mid: i for i, mid in enumerate(movie_ids)}
    idx_to_user = {i: uid for uid, i in user_to_idx.items()}
    idx_to_movie = {i: mid for mid, i in movie_to_idx.items()}

    n_users = len(user_ids)
    n_movies = len(movie_ids)
    print(f"  Users: {n_users:,}, Movies: {n_movies:,}, Ratings: {len(ratings):,}")

    # Build sparse user-item rating matrix
    row_indices = ratings["userId"].map(user_to_idx).values
    col_indices = ratings["movieId"].map(movie_to_idx).values
    values = ratings["rating"].values.astype(np.float32)

    user_item_sparse = csr_matrix(
        (values, (row_indices, col_indices)),
        shape=(n_users, n_movies),
        dtype=np.float32
    )

    # Compute means for score calibration/evaluation diagnostics
    user_rating_sums = np.array(user_item_sparse.sum(axis=1)).flatten()
    user_rating_counts = np.array((user_item_sparse > 0).sum(axis=1)).flatten()
    user_rating_counts[user_rating_counts == 0] = 1
    user_means = user_rating_sums / user_rating_counts
    global_mean = float(ratings["rating"].mean())

    # Build confidence matrix for implicit ALS: c_ui = 1 + alpha * (rating / 5)
    confidence = user_item_sparse.copy().astype(np.float32)
    confidence.data = 1.0 + ALS_ALPHA * (confidence.data / 5.0)
    item_user_conf = confidence.T.tocsr()

    n_factors = max(2, min(N_ALS_FACTORS, min(n_users, n_movies) - 1))
    print(f"  Running implicit ALS with {n_factors} factors...")
    als = AlternatingLeastSquares(
        factors=n_factors,
        regularization=ALS_REGULARIZATION,
        iterations=ALS_ITERATIONS,
        random_state=42,
    )
    als.fit(item_user_conf)

    # implicit returns user/item factors in dense arrays
    user_factors = als.user_factors.astype(np.float32)  # (n_users, n_factors)
    item_factors = als.item_factors.astype(np.float32)  # (n_movies, n_factors)

    # Save all collaborative model components
    collab_model = {
        "model_type": "implicit_als",
        "user_factors": user_factors,         # (n_users, n_factors)
        "item_factors": item_factors,         # (n_movies, n_factors)
        "user_to_idx": user_to_idx,
        "movie_to_idx": movie_to_idx,
        "idx_to_user": idx_to_user,
        "idx_to_movie": idx_to_movie,
        "user_means": user_means,
        "global_mean": global_mean,
        "als_alpha": ALS_ALPHA,
        "als_regularization": ALS_REGULARIZATION,
        "als_iterations": ALS_ITERATIONS,
    }

    with open(os.path.join(MODEL_DIR, "collab_model.pkl"), "wb") as f:
        pickle.dump(collab_model, f)

    print("  ✅ Collaborative model saved")
    return collab_model


def build_hybrid_model(movies: pd.DataFrame):
    """Precompute popularity scores for hybrid weighting."""
    print("\n── Popularity Normalization ──")

    scaler = MinMaxScaler()
    movies = movies.copy()
    pop_vals = movies["popularity"].values.reshape(-1, 1)
    vote_vals = movies["vote_average"].values.reshape(-1, 1)

    movies["popularity_normalized"] = scaler.fit_transform(pop_vals).flatten()
    movies["vote_normalized"] = MinMaxScaler().fit_transform(vote_vals).flatten()

    popularity_scores = {}
    for _, row in movies.iterrows():
        popularity_scores[row["movieId"]] = {
            "popularity_normalized": row["popularity_normalized"],
            "vote_normalized": row["vote_normalized"],
        }

    with open(os.path.join(MODEL_DIR, "popularity_scores.pkl"), "wb") as f:
        pickle.dump(popularity_scores, f)

    print("  ✅ Popularity scores saved")
    return popularity_scores


def save_movies_metadata(movies: pd.DataFrame):
    """Save clean movies metadata for engine use."""
    with open(os.path.join(MODEL_DIR, "movies_metadata.pkl"), "wb") as f:
        pickle.dump(movies, f)
    print("  ✅ Movies metadata saved")


def test_recommendations(movies: pd.DataFrame):
    """Quick sanity test of the models."""
    print("\n── Sanity Test ──")

    # Load models
    with open(os.path.join(MODEL_DIR, "tfidf_matrix.pkl"), "rb") as f:
        tfidf_matrix = pickle.load(f)
    with open(os.path.join(MODEL_DIR, "movie_indices.pkl"), "rb") as f:
        movie_indices = pickle.load(f)
    with open(os.path.join(MODEL_DIR, "collab_model.pkl"), "rb") as f:
        collab_model = pickle.load(f)

    # Test content-based: find similar movies to first movie
    from sklearn.metrics.pairwise import cosine_similarity
    test_movie_id = movies.iloc[0]["movieId"]
    if test_movie_id in movie_indices.index:
        idx = movie_indices[test_movie_id]
        sim_scores = cosine_similarity(tfidf_matrix[idx], tfidf_matrix).flatten()
        top_indices = sim_scores.argsort()[-6:][::-1]
        print(f"\n  Content-based: similar to '{movies.iloc[0]['title']}':")
        for i in top_indices[1:]:
            mid = movie_indices.index[i]
            title = movies[movies["movieId"] == mid]["title"].values
            if len(title) > 0:
                print(f"    → {title[0]} (sim: {sim_scores[i]:.3f})")

    # Test collaborative: predictions for first user
    first_user = list(collab_model["user_to_idx"].keys())[0]
    user_idx = collab_model["user_to_idx"][first_user]
    scores = collab_model["user_factors"][user_idx] @ collab_model["item_factors"].T
    top_movie_indices = scores.argsort()[-5:][::-1]
    print(f"\n  Collaborative: top recs for user {first_user}:")
    for mi in top_movie_indices:
        mid = collab_model["idx_to_movie"][mi]
        title = movies[movies["movieId"] == mid]["title"].values
        if len(title) > 0:
            print(f"    → {title[0]} (score: {scores[mi]:.3f})")


def main():
    print("=" * 60)
    print("PHASE 2: MODEL TRAINING")
    print("=" * 60)

    # Load processed data
    print("\nLoading processed data...")
    movies = pd.read_csv(os.path.join(DATA_DIR, "movies_merged.csv"))
    ratings = pd.read_csv(os.path.join(DATA_DIR, "ratings_subset.csv"))
    print(f"  Movies: {len(movies):,}, Ratings: {len(ratings):,}")

    # 1. Content-based model
    tfidf_matrix, movie_indices, movies_filtered = build_content_model(movies)

    # 2. Collaborative model
    collab_model = build_collaborative_model(ratings, movies_filtered)

    # 3. Popularity scores
    popularity_scores = build_hybrid_model(movies_filtered)

    # 4. Save metadata
    save_movies_metadata(movies_filtered)

    # 5. Quick test
    test_recommendations(movies_filtered)

    print("\n" + "=" * 60)
    print("MODEL TRAINING COMPLETE")
    print("=" * 60)
    print(f"  Models saved in: {MODEL_DIR}")
    for f_name in os.listdir(MODEL_DIR):
        size = os.path.getsize(os.path.join(MODEL_DIR, f_name))
        print(f"    {f_name}: {size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
