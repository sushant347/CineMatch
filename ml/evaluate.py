"""
ML Model Evaluation
====================
Evaluates hybrid recommendation model using Precision@K, Recall@K,
NDCG@K, RMSE, and catalog coverage.
"""

import os
import json
import pickle
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import cosine_similarity

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
MODEL_DIR = os.path.join(BASE_DIR, "ml", "models")
K = 10


def minmax_normalize(values: np.ndarray) -> np.ndarray:
    """Normalize a vector to 0-1 with safe handling for constant vectors."""
    v_min = float(values.min())
    v_max = float(values.max())
    if v_max > v_min:
        return (values - v_min) / (v_max - v_min)
    return np.zeros_like(values)


def get_item_factors_by_movie(collab: dict) -> np.ndarray:
    """Return item factor matrix in shape (n_movies, n_factors)."""
    n_movies = len(collab["idx_to_movie"])
    item_factors = collab["item_factors"]

    if item_factors.shape[0] == n_movies:
        return item_factors
    if item_factors.shape[1] == n_movies:
        return item_factors.T

    raise ValueError("Unexpected item_factors shape in collab model")


def predict_collab_scores(collab: dict, user_idx: int) -> np.ndarray:
    """Predict raw collaborative scores for all movies for one user."""
    user_vec = collab["user_factors"][user_idx]
    item_factors = collab["item_factors"]
    n_movies = len(collab["idx_to_movie"])

    if item_factors.shape[0] == n_movies:
        return user_vec @ item_factors.T
    return user_vec @ item_factors


def ndcg_at_k(binary_relevance: list, liked_count: int, k: int) -> float:
    """Compute NDCG@K for binary relevance labels."""
    if liked_count <= 0:
        return 0.0

    dcg = 0.0
    for i, rel in enumerate(binary_relevance[:k]):
        if rel:
            dcg += 1.0 / np.log2(i + 2)

    ideal_hits = min(liked_count, k)
    idcg = sum(1.0 / np.log2(i + 2) for i in range(ideal_hits))
    return float(dcg / idcg) if idcg > 0 else 0.0


def top_k_movie_ids(scores: np.ndarray, rated_set: set, movie_ids: list, k: int) -> list:
    """Fast top-K retrieval while filtering already-rated items."""
    if len(scores) == 0:
        return []

    # Pull a wide candidate window first, then optionally fall back to full sort.
    candidate_size = min(len(scores), max(k * 20, 200))
    top_idx = np.argpartition(scores, -candidate_size)[-candidate_size:]
    ranked_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

    recs = []
    for idx in ranked_idx:
        movie_id = movie_ids[idx]
        if movie_id in rated_set:
            continue
        recs.append(movie_id)
        if len(recs) >= k:
            return recs

    # Rare fallback if many top candidates are filtered out.
    for idx in np.argsort(scores)[::-1]:
        movie_id = movie_ids[idx]
        if movie_id in rated_set or movie_id in recs:
            continue
        recs.append(movie_id)
        if len(recs) >= k:
            break

    return recs


def load_models():
    """Load all trained models."""
    models = {}
    for name in ["collab_model", "tfidf_matrix", "movie_indices",
                  "popularity_scores", "movies_metadata"]:
        path = os.path.join(MODEL_DIR, f"{name}.pkl")
        with open(path, "rb") as f:
            models[name] = pickle.load(f)
    return models


def build_eval_resources(models: dict) -> dict:
    """Precompute static aligned vectors/mappings used across evaluations."""
    collab = models["collab_model"]
    movie_indices = models["movie_indices"]
    popularity_scores = models["popularity_scores"]

    n_movies = len(collab["idx_to_movie"])
    collab_movie_ids = [collab["idx_to_movie"][i] for i in range(n_movies)]

    popularity_vector = np.array([
        popularity_scores.get(mid, {}).get("popularity_normalized", 0.0)
        for mid in collab_movie_ids
    ], dtype=np.float32)
    popularity_vector = minmax_normalize(popularity_vector)

    collab_to_content = np.array([
        int(movie_indices[mid]) if mid in movie_indices.index else -1
        for mid in collab_movie_ids
    ], dtype=np.int32)

    item_factors = get_item_factors_by_movie(collab).astype(np.float32)
    item_norms = np.linalg.norm(item_factors, axis=1, keepdims=True)
    item_norms[item_norms == 0] = 1.0
    item_factors_norm = item_factors / item_norms

    movies_df = models["movies_metadata"]
    language_map = dict(zip(movies_df["movieId"], movies_df["original_language"]))
    is_hindi_mask = np.array([
        language_map.get(mid, "") == "hi" for mid in collab_movie_ids
    ], dtype=bool)

    return {
        "collab": collab,
        "tfidf_matrix": models["tfidf_matrix"],
        "movie_indices": movie_indices,
        "collab_movie_ids": collab_movie_ids,
        "collab_to_content": collab_to_content,
        "popularity_vector": popularity_vector,
        "item_factors_norm": item_factors_norm,
        "is_hindi_mask": is_hindi_mask,
        "movie_to_idx": collab["movie_to_idx"],
        "language_map": language_map,
        "catalog_size": int(len(movies_df)),
    }


def build_content_vector(liked_movie_ids: set, resources: dict) -> np.ndarray:
    """Compute content similarity vector aligned to collab movie indices."""
    tfidf_matrix = resources["tfidf_matrix"]
    movie_indices = resources["movie_indices"]
    collab_to_content = resources["collab_to_content"]

    liked_indices = [movie_indices[mid] for mid in liked_movie_ids if mid in movie_indices.index]
    if not liked_indices:
        return np.zeros(len(collab_to_content), dtype=np.float32)

    sims = cosine_similarity(tfidf_matrix[liked_indices], tfidf_matrix).mean(axis=0)
    content_vector = np.zeros(len(collab_to_content), dtype=np.float32)

    valid_mask = collab_to_content >= 0
    content_vector[valid_mask] = sims[collab_to_content[valid_mask]]
    return minmax_normalize(content_vector)


def build_item_item_fallback(user_history: list, resources: dict) -> np.ndarray | None:
    """Item-item fallback scores for sparse users (1-3 ratings)."""
    if not (1 <= len(user_history) <= 3):
        return None

    item_factors_norm = resources["item_factors_norm"]
    movie_to_idx = resources["movie_to_idx"]
    scores = np.zeros(item_factors_norm.shape[0], dtype=np.float32)

    total_weight = 0.0
    for movie_id, rating in user_history:
        movie_idx = movie_to_idx.get(int(movie_id))
        if movie_idx is None:
            continue

        sims = item_factors_norm @ item_factors_norm[movie_idx]
        weight = max(float(rating) - 2.5, 0.1)
        scores += weight * sims
        total_weight += weight

    if total_weight == 0:
        return None

    scores /= total_weight
    return minmax_normalize(scores)


def build_user_cache(eval_users: np.ndarray, ratings: pd.DataFrame, resources: dict) -> tuple[list, list]:
    """Precompute user vectors once so weight search is fast."""
    collab = resources["collab"]
    language_map = resources["language_map"]
    movie_to_idx = resources["movie_to_idx"]

    by_user = {uid: grp for uid, grp in ratings.groupby("userId")}
    cache = []
    rmse_pairs = []

    for user_id in eval_users:
        if user_id not in collab["user_to_idx"] or user_id not in by_user:
            continue

        user_ratings = by_user[user_id]
        liked_set = set(user_ratings[user_ratings["rating"] >= 4.0]["movieId"].values)
        if len(liked_set) < 2:
            continue

        rated_set = set(user_ratings["movieId"].values)
        history = list(zip(user_ratings["movieId"].values, user_ratings["rating"].values))
        user_idx = collab["user_to_idx"][user_id]

        collab_raw = predict_collab_scores(collab, user_idx)
        collab_norm = minmax_normalize(collab_raw.astype(np.float32))
        content_norm = build_content_vector(liked_set, resources)
        item_fallback = build_item_item_fallback(history, resources)

        hindi_count = sum(1 for movie_id in rated_set if language_map.get(movie_id, "") == "hi")
        prefers_hindi = hindi_count >= 3

        # RMSE from collaborative predictions mapped to rating range.
        eval_rows = user_ratings[user_ratings["movieId"].isin(movie_to_idx)]
        if not eval_rows.empty:
            eval_movie_idx = eval_rows["movieId"].map(movie_to_idx).astype(int).values
            pred_raw = collab_raw[eval_movie_idx]

            raw_min = float(pred_raw.min())
            raw_max = float(pred_raw.max())
            if raw_max > raw_min:
                pred_scaled = 0.5 + 4.5 * ((pred_raw - raw_min) / (raw_max - raw_min))
            else:
                user_mean = float(collab.get("user_means", np.array([3.0]))[user_idx])
                pred_scaled = np.full_like(pred_raw, user_mean, dtype=np.float32)

            pred_scaled = np.clip(pred_scaled, 0.5, 5.0)
            rmse_pairs.extend(zip(eval_rows["rating"].values, pred_scaled))

        cache.append({
            "liked_set": liked_set,
            "rated_set": rated_set,
            "collab_norm": collab_norm,
            "content_norm": content_norm,
            "item_fallback": item_fallback,
            "prefers_hindi": prefers_hindi,
        })

    return cache, rmse_pairs


def evaluate_weights(cache: list, resources: dict, weights: dict, k: int = 10) -> dict:
    """Evaluate one set of hybrid weights over cached user vectors."""
    popularity_vector = resources["popularity_vector"]
    collab_movie_ids = resources["collab_movie_ids"]
    is_hindi_mask = resources["is_hindi_mask"]

    precisions = []
    recalls = []
    ndcgs = []
    catalog_hits = set()

    for user in cache:
        scores = (
            weights["collab"] * user["collab_norm"]
            + weights["content"] * user["content_norm"]
            + weights["popularity"] * popularity_vector
        )

        if user["item_fallback"] is not None:
            scores = 0.7 * scores + 0.3 * user["item_fallback"]

        if user["prefers_hindi"]:
            scores = scores.copy()
            scores[is_hindi_mask] *= 1.1

        rec_ids = top_k_movie_ids(scores, user["rated_set"], collab_movie_ids, k)
        if not rec_ids:
            continue

        binary_rel = [1 if mid in user["liked_set"] else 0 for mid in rec_ids]
        hits = int(sum(binary_rel))

        precisions.append(hits / k)
        recalls.append(hits / len(user["liked_set"]))
        ndcgs.append(ndcg_at_k(binary_rel, len(user["liked_set"]), k))

        catalog_hits.update(rec_ids)

    return {
        "precision_at_10": float(np.mean(precisions)) if precisions else 0.0,
        "recall_at_10": float(np.mean(recalls)) if recalls else 0.0,
        "ndcg_at_10": float(np.mean(ndcgs)) if ndcgs else 0.0,
        "catalog_coverage": (
            float(len(catalog_hits) / resources["catalog_size"])
            if resources["catalog_size"] else 0.0
        ),
        "users_with_ratings": len(precisions),
    }


def weight_grid() -> list:
    """Simple hybrid-weight grid centered around 60/30/10 baseline."""
    return [
        {"collab": 0.7, "content": 0.2, "popularity": 0.1},
        {"collab": 0.6, "content": 0.3, "popularity": 0.1},
        {"collab": 0.5, "content": 0.4, "popularity": 0.1},
        {"collab": 0.4, "content": 0.5, "popularity": 0.1},
        {"collab": 0.6, "content": 0.2, "popularity": 0.2},
        {"collab": 0.5, "content": 0.3, "popularity": 0.2},
        {"collab": 0.4, "content": 0.4, "popularity": 0.2},
    ]


def evaluate():
    """Run full evaluation."""
    print("=" * 60)
    print("PHASE 3: MODEL EVALUATION")
    print("=" * 60)

    models = load_models()
    collab = models["collab_model"]
    resources = build_eval_resources(models)
    ratings = pd.read_csv(os.path.join(DATA_DIR, "ratings_subset.csv"))

    # Filter to users in the collaborative model
    eval_users = [u for u in ratings["userId"].unique() if u in collab["user_to_idx"]]
    # Sample users for evaluation speed
    np.random.seed(42)
    eval_users = np.random.choice(eval_users, size=min(500, len(eval_users)), replace=False)

    print(f"\nEvaluating on {len(eval_users)} users...")

    user_cache, rmse_values = build_user_cache(eval_users, ratings, resources)

    print(f"  Users with enough history: {len(user_cache)}")
    print("\nRunning weight grid search...")

    grid_results = []
    best = None
    for weights in weight_grid():
        metrics = evaluate_weights(user_cache, resources, weights, k=K)
        record = {
            "weights": weights,
            "precision_at_10": round(metrics["precision_at_10"], 4),
            "recall_at_10": round(metrics["recall_at_10"], 4),
            "ndcg_at_10": round(metrics["ndcg_at_10"], 4),
            "catalog_coverage": round(metrics["catalog_coverage"], 4),
        }
        grid_results.append(record)

        print(
            "  "
            f"w=({weights['collab']:.1f}/{weights['content']:.1f}/{weights['popularity']:.1f}) "
            f"P@10={record['precision_at_10']:.4f} "
            f"NDCG@10={record['ndcg_at_10']:.4f}"
        )

        score_key = (metrics["precision_at_10"], metrics["ndcg_at_10"])
        if best is None or score_key > best["score_key"]:
            best = {
                "score_key": score_key,
                "weights": weights,
                "metrics": metrics,
            }

    if rmse_values:
        actuals, preds = zip(*rmse_values)
        rmse = float(np.sqrt(mean_squared_error(actuals, preds)))
    else:
        rmse = 0.0

    results = {
        "precision_at_10": round(best["metrics"]["precision_at_10"], 4),
        "recall_at_10": round(best["metrics"]["recall_at_10"], 4),
        "ndcg_at_10": round(best["metrics"]["ndcg_at_10"], 4),
        "catalog_coverage": round(best["metrics"]["catalog_coverage"], 4),
        "rmse": round(rmse, 4),
        "users_evaluated": int(len(eval_users)),
        "users_with_ratings": int(best["metrics"]["users_with_ratings"]),
        "best_hybrid_weights": best["weights"],
        "grid_search": grid_results,
    }

    print(f"\n  📊 EVALUATION RESULTS:")
    print(f"  ─────────────────────")
    print(f"  Precision@{K}: {results['precision_at_10']}")
    print(f"  Recall@{K}:    {results['recall_at_10']}")
    print(f"  NDCG@{K}:      {results['ndcg_at_10']}")
    print(f"  Coverage:      {results['catalog_coverage']}")
    print(f"  RMSE:          {results['rmse']}")
    print(f"  Users tested:  {results['users_with_ratings']}")
    print(
        "  Best Weights: "
        f"collab={results['best_hybrid_weights']['collab']}, "
        f"content={results['best_hybrid_weights']['content']}, "
        f"popularity={results['best_hybrid_weights']['popularity']}"
    )

    # Save
    out_path = os.path.join(BASE_DIR, "ml", "evaluation_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    weights_out = os.path.join(MODEL_DIR, "hybrid_weights.json")
    with open(weights_out, "w") as f:
        json.dump(results["best_hybrid_weights"], f, indent=2)

    print(f"\n  ✅ Results saved to {out_path}")
    print(f"  ✅ Best weights saved to {weights_out}")

    return results


if __name__ == "__main__":
    evaluate()
