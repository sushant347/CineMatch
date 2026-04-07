"""
Hybrid Recommendation Engine
==============================
Loads ML models and provides recommendation functions.
Combines collaborative filtering, content-based, and popularity scoring.
"""

import json
import os
import pickle
import numpy as np
from django.conf import settings
from django.core.cache import cache
from sklearn.metrics.pairwise import cosine_similarity


RECOMMENDATION_CACHE_TTL_SECONDS = 900
SIMILAR_CACHE_TTL_SECONDS = 3600
TRENDING_CACHE_TTL_SECONDS = 1800
USER_HISTORY_CACHE_TTL_SECONDS = 120


class RecommendationEngine:
    """Singleton recommendation engine that loads models once."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._loaded = False
        return cls._instance

    def load_models(self):
        """Load all pre-trained ML model artifacts."""
        if self._loaded:
            return

        model_dir = settings.ML_MODELS_DIR
        print(f"[Engine] Loading ML models from {model_dir}...")

        with open(os.path.join(model_dir, "tfidf_matrix.pkl"), "rb") as f:
            self.tfidf_matrix = pickle.load(f)

        with open(os.path.join(model_dir, "movie_indices.pkl"), "rb") as f:
            self.movie_indices = pickle.load(f)

        with open(os.path.join(model_dir, "collab_model.pkl"), "rb") as f:
            self.collab = pickle.load(f)

        with open(os.path.join(model_dir, "popularity_scores.pkl"), "rb") as f:
            self.popularity_scores = pickle.load(f)

        with open(os.path.join(model_dir, "movies_metadata.pkl"), "rb") as f:
            self.movies_df = pickle.load(f)

        self.collab_movie_ids = [
            self.collab["idx_to_movie"][i]
            for i in range(len(self.collab["idx_to_movie"]))
        ]

        self.movie_language_map = dict(
            zip(self.movies_df["movieId"], self.movies_df["original_language"])
        )

        self.popularity_vector = np.array([
            self.popularity_scores.get(mid, {}).get("popularity_normalized", 0.0)
            for mid in self.collab_movie_ids
        ], dtype=np.float32)
        self.popularity_vector = self._minmax(self.popularity_vector)

        self.collab_to_content = np.array([
            int(self.movie_indices[mid]) if mid in self.movie_indices.index else -1
            for mid in self.collab_movie_ids
        ], dtype=np.int32)

        self.item_factors_by_movie = self._get_item_factors_by_movie().astype(np.float32)
        norms = np.linalg.norm(self.item_factors_by_movie, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self.item_factors_norm = self.item_factors_by_movie / norms

        self.is_hindi_mask = np.array([
            self.movie_language_map.get(mid, "") == "hi" for mid in self.collab_movie_ids
        ], dtype=bool)

        self.hybrid_weights = {
            "collab": 0.6,
            "content": 0.3,
            "popularity": 0.1,
        }
        weights_path = os.path.join(model_dir, "hybrid_weights.json")
        if os.path.exists(weights_path):
            try:
                with open(weights_path, "r", encoding="utf-8") as f:
                    loaded_weights = json.load(f)

                if all(k in loaded_weights for k in ["collab", "content", "popularity"]):
                    self.hybrid_weights = {
                        "collab": float(loaded_weights["collab"]),
                        "content": float(loaded_weights["content"]),
                        "popularity": float(loaded_weights["popularity"]),
                    }
            except (json.JSONDecodeError, OSError, ValueError):
                # Keep defaults when weights cannot be loaded.
                pass

        self._loaded = True
        print(f"[Engine] Models loaded. {len(self.movies_df)} movies available.")

    @staticmethod
    def _minmax(values):
        """Normalize a numeric vector to 0-1 range safely."""
        v_min = float(values.min())
        v_max = float(values.max())
        if v_max > v_min:
            return (values - v_min) / (v_max - v_min)
        return np.zeros_like(values)

    def _get_item_factors_by_movie(self):
        """Return item factors as (n_movies, n_factors) for both ALS/SVD artifacts."""
        item_factors = self.collab["item_factors"]
        n_movies = len(self.collab_movie_ids)

        if item_factors.shape[0] == n_movies:
            return item_factors
        if item_factors.shape[1] == n_movies:
            return item_factors.T

        raise ValueError("Unexpected item_factors shape in collaborative model")

    def _predict_collab_scores(self, user_idx):
        """Predict collaborative scores for every movie for one user."""
        user_vec = self.collab["user_factors"][user_idx]
        item_factors = self.collab["item_factors"]
        n_movies = len(self.collab_movie_ids)

        if item_factors.shape[0] == n_movies:
            return user_vec @ item_factors.T
        return user_vec @ item_factors

    def _build_content_scores(self, liked_movie_ids):
        """Build content-based score vector aligned with collab movie ordering."""
        liked_indices = [
            self.movie_indices[mid] for mid in liked_movie_ids if mid in self.movie_indices.index
        ]
        if not liked_indices:
            return np.zeros(len(self.collab_movie_ids), dtype=np.float32)

        sims = cosine_similarity(self.tfidf_matrix[liked_indices], self.tfidf_matrix).mean(axis=0)
        content_scores = np.zeros(len(self.collab_movie_ids), dtype=np.float32)
        valid_mask = self.collab_to_content >= 0
        content_scores[valid_mask] = sims[self.collab_to_content[valid_mask]]
        return self._minmax(content_scores)

    def _build_item_item_scores(self, user_history):
        """Item-item fallback scoring for users with sparse history."""
        if not (1 <= len(user_history) <= 3):
            return None

        scores = np.zeros(len(self.collab_movie_ids), dtype=np.float32)
        total_weight = 0.0

        for movie_id, rating in user_history:
            movie_idx = self.collab["movie_to_idx"].get(int(movie_id))
            if movie_idx is None:
                continue

            sims = self.item_factors_norm @ self.item_factors_norm[movie_idx]
            weight = max(float(rating) - 2.5, 0.1)
            scores += weight * sims
            total_weight += weight

        if total_weight == 0:
            return None

        scores /= total_weight
        return self._minmax(scores)

    def _top_recommendations_from_scores(self, scores, rated_set, n, reason=""):
        """Convert score vector to ranked recommendation payload."""
        if len(scores) == 0:
            return []

        candidate_size = min(len(scores), max(n * 20, 200))
        top_idx = np.argpartition(scores, -candidate_size)[-candidate_size:]
        ranked_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

        results = []
        for idx in ranked_idx:
            movie_id = self.collab_movie_ids[idx]
            if movie_id in rated_set:
                continue

            results.append({
                "movie_id": int(movie_id),
                "score": float(scores[idx]),
                "reason": reason,
            })
            if len(results) >= n:
                return results

        for idx in np.argsort(scores)[::-1]:
            movie_id = self.collab_movie_ids[idx]
            if movie_id in rated_set:
                continue
            if any(item["movie_id"] == int(movie_id) for item in results):
                continue

            results.append({
                "movie_id": int(movie_id),
                "score": float(scores[idx]),
                "reason": reason,
            })
            if len(results) >= n:
                break

        return results

    def _user_prefers_hindi(self, rated_movie_ids):
        """Detect Hindi preference from rating history."""
        hindi_count = sum(1 for mid in rated_movie_ids if self.movie_language_map.get(mid, "") == "hi")
        return hindi_count >= 3

    def _get_user_history_bundle(self, user_id):
        """Fetch and cache normalized user history used by multiple recommendation flows."""
        normalized_user_id = int(user_id)
        cache_key = f"user-history:{normalized_user_id}"
        try:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
        except Exception:
            cached = None

        from .models import Rating, WatchedMovie, UserAccount

        is_custom_user = UserAccount.objects.filter(user_id=normalized_user_id).exists()
        rating_divisor = 2.0 if is_custom_user else 1.0

        raw_user_history = list(
            Rating.objects.filter(user_id=normalized_user_id).values_list("movie_id", "rating")
        )
        user_history = [
            (int(movie_id), float(rating) / rating_divisor)
            for movie_id, rating in raw_user_history
        ]
        watched_movie_ids = list(
            WatchedMovie.objects.filter(user_id=normalized_user_id).values_list("movie_id", flat=True)
        )

        bundle = {
            "is_custom_user": bool(is_custom_user),
            "user_history": user_history,
            "watched_movie_ids": [int(movie_id) for movie_id in watched_movie_ids],
        }

        try:
            cache.set(cache_key, bundle, timeout=USER_HISTORY_CACHE_TTL_SECONDS)
        except Exception:
            pass

        return bundle

    def get_similar_movies(self, movie_id, n=10):
        """Content-based: find similar movies using TF-IDF cosine similarity."""
        self.load_models()

        cache_key = f"similar:{movie_id}"
        cache_batch_size = max(n, 40)
        try:
            cached = cache.get(cache_key)
            if cached is not None and len(cached) >= n:
                return cached[:n]
        except Exception:
            cached = None

        if movie_id not in self.movie_indices.index:
            return []

        idx = self.movie_indices[movie_id]
        sim_scores = cosine_similarity(
            self.tfidf_matrix[idx], self.tfidf_matrix
        ).flatten()

        # Get top similar (exclude itself)
        top_indices = sim_scores.argsort()[::-1][1:cache_batch_size + 1]

        results = []
        for i in top_indices:
            mid = self.movie_indices.index[i]
            results.append({
                "movie_id": int(mid),
                "score": float(sim_scores[i]),
                "reason": f"Similar content to movie you're viewing",
            })

        try:
            cache.set(cache_key, results, timeout=SIMILAR_CACHE_TTL_SECONDS)
        except Exception:
            pass

        return results[:n]

    def get_user_recommendations(self, user_id, n=20):
        """Hybrid: combine collaborative + content + popularity for a user."""
        self.load_models()

        normalized_user_id = int(user_id)
        cache_key = f"recs:{normalized_user_id}"
        cache_batch_size = max(n, 50)
        try:
            cached = cache.get(cache_key)
            if cached is not None and len(cached) >= n:
                return cached[:n]
        except Exception:
            cached = None

        history_bundle = self._get_user_history_bundle(normalized_user_id)
        user_history = list(history_bundle.get("user_history") or [])
        watched_movie_ids = set(history_bundle.get("watched_movie_ids") or [])

        rating_by_movie = {int(mid): float(rating) for mid, rating in user_history}
        combined_history = list(user_history)
        for watched_mid in watched_movie_ids:
            if int(watched_mid) not in rating_by_movie:
                combined_history.append((int(watched_mid), 4.0))

        rated_movie_ids = {mid for mid, _ in combined_history}
        liked_movie_ids = {
            mid for mid, rating in combined_history if rating >= 4.0
        }

        collab = self.collab
        sparse_history = 1 <= len(combined_history) <= 3

        if normalized_user_id not in collab["user_to_idx"]:
            fallback_scores = self._build_item_item_scores(combined_history)
            if fallback_scores is not None:
                recs = self._top_recommendations_from_scores(
                    fallback_scores,
                    rated_movie_ids,
                    cache_batch_size,
                    reason="Based on similar movies to your early ratings",
                )
            else:
                recs = self._get_popular_recommendations(cache_batch_size)

            try:
                cache.set(cache_key, recs, timeout=RECOMMENDATION_CACHE_TTL_SECONDS)
            except Exception:
                pass
            return recs[:n]

        user_idx = collab["user_to_idx"][normalized_user_id]
        collab_scores = self._predict_collab_scores(user_idx)
        collab_norm = self._minmax(collab_scores)

        content_scores = self._build_content_scores(liked_movie_ids)

        hybrid_scores = (
            self.hybrid_weights["collab"] * collab_norm
            + self.hybrid_weights["content"] * content_scores
            + self.hybrid_weights["popularity"] * self.popularity_vector
        )

        item_item_scores = self._build_item_item_scores(combined_history)
        if sparse_history and item_item_scores is not None:
            hybrid_scores = 0.7 * hybrid_scores + 0.3 * item_item_scores

        if self._user_prefers_hindi(rated_movie_ids):
            hybrid_scores = hybrid_scores.copy()
            hybrid_scores[self.is_hindi_mask] *= 1.1

        reason = "Hybrid recommendation"
        if sparse_history and item_item_scores is not None:
            reason = "Hybrid recommendation with item-similarity fallback"

        recs = self._top_recommendations_from_scores(
            hybrid_scores,
            rated_movie_ids,
            cache_batch_size,
            reason=reason,
        )

        try:
            cache.set(cache_key, recs, timeout=RECOMMENDATION_CACHE_TTL_SECONDS)
        except Exception:
            pass

        return recs[:n]

    def get_explained_recommendations(self, user_id, n=20):
        """Get recommendations with 'Because you watched X' explanations."""
        self.load_models()

        from .models import Movie

        history_bundle = self._get_user_history_bundle(user_id)
        user_history = list(history_bundle.get("user_history") or [])
        liked_movies = [
            (int(movie_id), float(rating))
            for movie_id, rating in user_history
            if float(rating) >= 4.0
        ]
        if not liked_movies:
            return []

        liked_movies.sort(key=lambda item: item[1], reverse=True)
        top_rated_ids = [movie_id for movie_id, _ in liked_movies[:5]]
        source_movies = Movie.objects.in_bulk(top_rated_ids, field_name="movie_id")

        explained_groups = []
        seen_movies = set()

        for source_movie_id in top_rated_ids:
            similar = self.get_similar_movies(source_movie_id, n=6)
            source_movie = source_movies.get(int(source_movie_id))
            source_title = source_movie.title if source_movie else "a movie you liked"

            group_movies = []
            for item in similar:
                similar_movie_id = int(item["movie_id"])
                if similar_movie_id in seen_movies:
                    continue

                group_movies.append({
                    "movie_id": similar_movie_id,
                    "score": float(item.get("score", 0.0)),
                    "reason": f"Because you watched {source_title}",
                })
                seen_movies.add(similar_movie_id)

            if group_movies:
                explained_groups.append({
                    "source_title": source_title,
                    "source_movie_id": int(source_movie_id),
                    "recommendations": group_movies,
                })

        return explained_groups

    def _get_popular_recommendations(self, n=20):
        """Fallback: return most popular movies."""
        self.load_models()

        sorted_movies = self.movies_df.sort_values("popularity", ascending=False)
        results = []
        for _, row in sorted_movies.head(n).iterrows():
            results.append({
                "movie_id": int(row["movieId"]),
                "score": float(row["popularity"]),
                "reason": "Trending now",
            })
        return results

    def get_trending(self, n=20):
        """Get trending movies by popularity."""
        cache_key = "trending"
        cache_batch_size = max(n, 60)
        try:
            cached = cache.get(cache_key)
            if cached is not None and len(cached) >= n:
                return cached[:n]
        except Exception:
            cached = None

        recs = self._get_popular_recommendations(cache_batch_size)

        try:
            cache.set(cache_key, recs, timeout=TRENDING_CACHE_TTL_SECONDS)
        except Exception:
            pass

        return recs[:n]

    def get_hindi_movies(self, n=20):
        """Get popular Hindi movies."""
        self.load_models()

        cache_key = "hindi_movies"
        cache_batch_size = max(n, 60)
        try:
            cached = cache.get(cache_key)
            if cached is not None and len(cached) >= n:
                return cached[:n]
        except Exception:
            cached = None

        hindi = self.movies_df[
            self.movies_df["original_language"] == "hi"
        ].sort_values("popularity", ascending=False)

        results = []
        for _, row in hindi.head(cache_batch_size).iterrows():
            results.append({
                "movie_id": int(row["movieId"]),
                "score": float(row["popularity"]),
                "reason": "Popular Hindi movie",
            })

        try:
            cache.set(cache_key, results, timeout=TRENDING_CACHE_TTL_SECONDS)
        except Exception:
            pass

        return results[:n]

    def invalidate_user_cache(self, user_id):
        """Invalidate cached user recommendations when their ratings change."""
        normalized_user_id = int(user_id)
        cache_keys = [
            f"recs:{normalized_user_id}",
            f"user-history:{normalized_user_id}",
            f"api:home:user:{normalized_user_id}:v3",
            f"api:profile:{normalized_user_id}:v2",
        ]

        for cache_key in cache_keys:
            try:
                cache.delete(cache_key)
            except Exception:
                pass

    @staticmethod
    def _normalize_list(values, lower=False):
        if not values:
            return []

        cleaned = []
        seen = set()
        for value in values:
            text = str(value).strip()
            if not text:
                continue

            normalized = text.lower() if lower else text
            if normalized in seen:
                continue

            seen.add(normalized)
            cleaned.append(normalized)

        return cleaned

    def get_cold_start_recommendations(self, genres=None, languages=None, n=20):
        """Recommendations for new users based on genre/language preferences."""
        self.load_models()

        limit = max(int(n or 20), 1)
        genres = self._normalize_list(genres, lower=True)
        languages = self._normalize_list(languages, lower=True)
        if not languages:
            languages = ["en"]

        df = self.movies_df.copy()
        df["genres"] = df["genres"].fillna("")
        df["original_language"] = df["original_language"].fillna("").str.lower()
        df["popularity"] = df["popularity"].fillna(0.0)
        df["vote_average"] = df["vote_average"].fillna(0.0)

        def filter_by_genres(frame):
            if not genres or frame.empty:
                return frame.copy()

            mask = frame["genres"].astype(str).str.lower().apply(
                lambda as_text: any(genre in as_text for genre in genres)
            )
            return frame.loc[mask]

        language_df = df[df["original_language"].isin(languages)] if languages else df
        genre_df = filter_by_genres(df)

        if languages and genres:
            primary_df = filter_by_genres(language_df)
            primary_reason = "Matches your preferred genres and languages"
        elif genres:
            primary_df = genre_df
            primary_reason = "Matches your preferred genres"
        elif languages:
            primary_df = language_df
            primary_reason = "Matches your preferred languages"
        else:
            primary_df = df
            primary_reason = "Trending now"

        candidate_pools = [
            (primary_df, primary_reason),
        ]

        # If strict filters are sparse, backfill with partial matches before global trending.
        if languages and genres:
            candidate_pools.append((genre_df, "Matching your preferred genres"))
            candidate_pools.append((language_df, "Matching your preferred languages"))

        candidate_pools.append((df, "Trending now"))

        results = []
        seen_movie_ids = set()
        for pool_df, reason in candidate_pools:
            sort_columns = [
                column for column in ("popularity", "vote_average")
                if column in pool_df.columns
            ]
            ranked = pool_df.sort_values(sort_columns, ascending=False) if sort_columns else pool_df
            for _, row in ranked.iterrows():
                movie_id = int(row["movieId"])
                if movie_id in seen_movie_ids:
                    continue

                seen_movie_ids.add(movie_id)
                results.append({
                    "movie_id": movie_id,
                    "score": float(row["popularity"]),
                    "reason": reason,
                })

                if len(results) >= limit:
                    return results

        return results

    def search_movies(self, query, n=20):
        """Search movies by title (case-insensitive substring match)."""
        self.load_models()

        query_lower = query.lower().strip()
        matches = self.movies_df[
            self.movies_df["title"].str.lower().str.contains(query_lower, na=False)
        ].sort_values("popularity", ascending=False)

        results = []
        for _, row in matches.head(n).iterrows():
            results.append({
                "movie_id": int(row["movieId"]),
                "score": float(row["popularity"]),
                "reason": "",
            })
        return results


# Global engine instance
engine = RecommendationEngine()
