"""
Restore full movie catalog and import all JSON reviews per movie.

This command is designed for the scenario where:
- You want the app catalog restored to the larger/original size (default 26k).
- You want each matched movie to keep all review texts from JSONL source files.
- Review ownership should use only self-account users with real usernames.
"""

import json
import random
import re
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count

from recommender.models import (
    Movie,
    Rating,
    Review,
    UserAccount,
    UserProfile,
    Watchlist,
    WatchedMovie,
)


def clean_text(value):
    text = str(value or "").replace("\x00", " ").strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def clean_path(value):
    text = clean_text(value)
    if not text:
        return ""
    if not text.startswith("/"):
        return f"/{text}"
    return text


def parse_year_from_title(title):
    m = re.search(r"\((19\d{2}|20\d{2})\)\s*$", str(title or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def strip_trailing_year(title):
    return re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", str(title or "")).strip()


def normalize_title(title):
    text = re.sub(r"[^a-z0-9]+", " ", str(title or "").lower())
    return re.sub(r"\s+", " ", text).strip()


def safe_float(value, default=0.0):
    if value is None:
        return float(default)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return float(default)
    try:
        return float(text)
    except ValueError:
        return float(default)


def safe_int(value, default=None):
    if value is None:
        return default
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def parse_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        left = text.split("/", 1)[0].strip()
        try:
            return float(left)
        except ValueError:
            pass
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def extract_rating(record):
    for key in ("rating", "imdb_rating", "score", "stars", "vote_average"):
        if key not in record:
            continue
        value = parse_float(record.get(key))
        if value is None:
            continue
        if value > 5.0:
            value = value / 2.0
        value = max(0.5, min(5.0, value))
        value = round(value * 2.0) / 2.0
        return float(value)
    return None


class Command(BaseCommand):
    help = "Restore full catalog (default 26k) and import all reviews from JSONL files."
    DB_IN_CHUNK = 5000

    @staticmethod
    def _chunked(values, size):
        for i in range(0, len(values), size):
            yield values[i:i + size]

    def add_arguments(self, parser):
        parser.add_argument(
            "--movies-file",
            type=str,
            default=str(Path(settings.PROJECT_ROOT) / "data" / "processed" / "movies_merged.csv"),
            help="Primary movie metadata CSV (recommended: data/processed/movies_merged.csv)",
        )
        parser.add_argument(
            "--fallback-movies-file",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "movies.csv"),
            help="Fallback movies.csv when primary metadata file is unavailable.",
        )
        parser.add_argument(
            "--reviews-glob",
            type=str,
            default="data/raw/movie_reviews_all_10*.jsonl",
            help="Glob pattern (under project root) for review JSONL files.",
        )
        parser.add_argument(
            "--target-movies",
            type=int,
            default=26000,
            help="Number of movies to keep (default: 26000).",
        )
        parser.add_argument(
            "--min-review-length",
            type=int,
            default=20,
            help="Minimum review text length.",
        )
        parser.add_argument(
            "--min-self-users",
            type=int,
            default=32,
            help="Minimum self users to maintain.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Bulk operation batch size.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for deterministic user-review assignment.",
        )
        parser.add_argument(
            "--missing-output",
            type=str,
            default=str(Path(settings.PROJECT_ROOT) / "movie.md"),
            help="File path to write movies that still have no reviews.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Plan and report without writing DB/file changes.",
        )

    def _load_movie_rows(self, primary_path: Path, fallback_path: Path, target_movies: int):
        source_path = primary_path if primary_path.exists() else fallback_path
        if not source_path.exists():
            raise CommandError(
                f"No movie source found. Tried: {primary_path} and {fallback_path}"
            )

        self.stdout.write(f"  Loading movies from: {source_path}")
        df = pd.read_csv(source_path, low_memory=False)
        if "movieId" not in df.columns or "title" not in df.columns:
            raise CommandError("Movie source must contain at least movieId and title columns.")

        df = df.dropna(subset=["movieId", "title"])
        df["movieId"] = pd.to_numeric(df["movieId"], errors="coerce")
        df = df.dropna(subset=["movieId"])
        df["movieId"] = df["movieId"].astype(int)
        df = df.drop_duplicates(subset=["movieId"], keep="first").sort_values("movieId")

        if target_movies > 0 and len(df) > target_movies:
            df = df.head(target_movies)

        rows = []
        for item in df.to_dict("records"):
            title = clean_text(item.get("title"))
            year_val = safe_int(item.get("year"), default=None)
            if year_val is None:
                year_val = parse_year_from_title(title)

            rows.append(
                {
                    "movie_id": int(item["movieId"]),
                    "tmdb_id": safe_int(item.get("tmdbId"), default=None),
                    "title": title,
                    "year": year_val,
                    "genres": clean_text(item.get("genres")),
                    "overview": clean_text(item.get("overview")),
                    "original_language": (clean_text(item.get("original_language")) or "en").lower(),
                    "popularity": safe_float(item.get("popularity"), default=0.0),
                    "vote_average": safe_float(item.get("vote_average"), default=0.0),
                    "poster_path": clean_path(item.get("poster_path")),
                    "backdrop_path": clean_path(item.get("backdrop_path")),
                    "keywords": clean_text(item.get("keywords")),
                    "release_date": clean_text(item.get("release_date")),
                    "tagline": clean_text(item.get("tagline")),
                }
            )

        if not rows:
            raise CommandError("No usable movie rows were loaded from source CSV.")
        return rows, source_path

    def _sync_movies(self, rows, batch_size: int, dry_run: bool):
        fields = [
            "tmdb_id",
            "title",
            "year",
            "genres",
            "overview",
            "original_language",
            "popularity",
            "vote_average",
            "poster_path",
            "backdrop_path",
            "keywords",
            "release_date",
            "tagline",
        ]

        target_ids = [row["movie_id"] for row in rows]
        target_set = set(target_ids)

        existing = {}
        for chunk in self._chunked(target_ids, self.DB_IN_CHUNK):
            for obj in Movie.objects.filter(movie_id__in=chunk):
                existing[obj.movie_id] = obj

        to_create = []
        to_update = []

        for row in rows:
            movie_id = row["movie_id"]
            obj = existing.get(movie_id)
            if obj is None:
                to_create.append(Movie(**row))
                continue

            dirty = False
            for field in fields:
                new_val = row.get(field)
                if getattr(obj, field) != new_val:
                    setattr(obj, field, new_val)
                    dirty = True
            if dirty:
                to_update.append(obj)

        remove_qs = None
        remove_count = 0
        if len(target_ids) <= self.DB_IN_CHUNK:
            remove_qs = Movie.objects.exclude(movie_id__in=target_set)
            remove_count = remove_qs.count()

        if not dry_run:
            if to_create:
                Movie.objects.bulk_create(to_create, batch_size=batch_size, ignore_conflicts=True)
            if to_update:
                Movie.objects.bulk_update(to_update, fields, batch_size=batch_size)
            if remove_qs is not None and remove_count:
                remove_qs.delete()

        return {
            "target_ids": target_ids,
            "target_set": target_set,
            "created": len(to_create),
            "updated": len(to_update),
            "removed": remove_count,
        }

    def _gather_review_files(self, reviews_glob: str):
        files = sorted(Path(settings.PROJECT_ROOT).glob(reviews_glob))
        if not files:
            raise CommandError(
                f"No review files matched pattern: {reviews_glob}"
            )
        return files

    def _parse_reviews(self, review_files, target_rows, min_review_length: int):
        exact_key_map = {}
        normalized_key_map = {}
        normalized_title_map = {}
        imdb_movie_id_map = {}

        for row in target_rows:
            movie_id = int(row["movie_id"])
            title = clean_text(row.get("title"))
            year = row.get("year")
            title_wo_year = strip_trailing_year(title)

            # Some catalogs use IMDb numeric ids directly as movie_id.
            imdb_movie_id_map[movie_id] = movie_id

            norm_variants = {
                normalize_title(title),
                normalize_title(title_wo_year),
            }
            norm_variants.discard("")

            if year:
                exact_key_map[f"{title}|{int(year)}"] = movie_id
                exact_key_map[f"{title_wo_year}|{int(year)}"] = movie_id
                for norm in norm_variants:
                    normalized_key_map[(norm, int(year))] = movie_id

            for norm in norm_variants:
                normalized_title_map[norm] = movie_id

        movie_reviews = defaultdict(list)
        movie_rating_hint = {}
        parsed_lines = 0
        matched_lines = 0
        imdb_matches = 0

        for path in review_files:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    parsed_lines += 1

                    try:
                        rec = json.loads(text)
                    except json.JSONDecodeError:
                        continue

                    reviews_raw = rec.get("reviews")
                    if not isinstance(reviews_raw, list):
                        reviews_raw = []

                    cleaned_reviews = []
                    for item in reviews_raw:
                        rv = clean_text(item)
                        if len(rv) >= min_review_length:
                            cleaned_reviews.append(rv[:2000])

                    movie_id = None

                    imdb_raw = clean_text(rec.get("imdb_id")).lower()
                    if imdb_raw:
                        imdb_digits = re.sub(r"[^0-9]", "", imdb_raw)
                        imdb_numeric = safe_int(imdb_digits, default=None)
                        if imdb_numeric is not None:
                            movie_id = imdb_movie_id_map.get(imdb_numeric)
                            if movie_id is not None:
                                imdb_matches += 1

                    if movie_id is None:
                        movie_key = clean_text(rec.get("movie_key"))
                        if movie_key and movie_key in exact_key_map:
                            movie_id = exact_key_map[movie_key]
                        elif movie_key and "|" in movie_key:
                            left, right = movie_key.rsplit("|", 1)
                            norm = normalize_title(strip_trailing_year(left))
                            year_int = safe_int(right, default=None)
                            if year_int is not None:
                                movie_id = normalized_key_map.get((norm, year_int))
                            if movie_id is None:
                                movie_id = normalized_title_map.get(norm)

                    if movie_id is None:
                        title = clean_text(rec.get("query_title") or rec.get("matched_title"))
                        if title:
                            title_norm = normalize_title(strip_trailing_year(title))
                            year = rec.get("query_year") or rec.get("matched_year")
                            year_int = safe_int(year, default=None)
                            if year_int is not None:
                                movie_id = normalized_key_map.get((title_norm, year_int))
                            if movie_id is None:
                                movie_id = normalized_title_map.get(title_norm)

                    if movie_id is None:
                        continue

                    matched_lines += 1
                    if cleaned_reviews:
                        movie_reviews[int(movie_id)].extend(cleaned_reviews)

                    hint = extract_rating(rec)
                    if hint is not None and int(movie_id) not in movie_rating_hint:
                        movie_rating_hint[int(movie_id)] = hint

        # De-duplicate reviews per movie while preserving order.
        deduped = {}
        max_reviews_per_movie = 0
        for movie_id, texts in movie_reviews.items():
            unique_texts = list(dict.fromkeys(texts))
            if unique_texts:
                deduped[movie_id] = unique_texts
                if len(unique_texts) > max_reviews_per_movie:
                    max_reviews_per_movie = len(unique_texts)

        return {
            "movie_reviews": deduped,
            "movie_rating_hint": movie_rating_hint,
            "parsed_lines": parsed_lines,
            "matched_lines": matched_lines,
            "imdb_matches": imdb_matches,
            "max_reviews_per_movie": max_reviews_per_movie,
        }

    def _ensure_real_users(self, required_count: int, dry_run: bool):
        required = max(2, int(required_count))

        first_names = [
            "Aarav", "Vivaan", "Aditya", "Arjun", "Ishaan", "Kabir", "Reyansh", "Vihaan",
            "Anaya", "Diya", "Ira", "Kiara", "Mira", "Aisha", "Myra", "Siya",
            "Rahul", "Rohan", "Vikram", "Aman", "Karan", "Neha", "Pooja", "Sneha",
            "Nikhil", "Ritika", "Tanvi", "Meera", "Varun", "Aditi", "Sanjay", "Ankit",
        ]
        last_names = [
            "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kapoor", "Mehta", "Nair",
            "Reddy", "Iyer", "Malhotra", "Bose", "Joshi", "Kulkarni", "Chawla", "Saxena",
            "Pandey", "Mishra", "Agarwal", "Kohli", "Das", "Roy", "Pillai", "Bhat",
        ]

        def looks_generic(username: str):
            value = (username or "").lower().strip()
            return (
                value.startswith("authtest")
                or value.startswith("pref_")
                or value.startswith("test")
                or bool(re.fullmatch(r"user\d+", value))
                or bool(re.fullmatch(r"member\d+", value))
            )

        accounts = list(UserAccount.objects.order_by("user_id"))
        used_names = {acc.username.lower() for acc in accounts}

        def candidate_names():
            for fn in first_names:
                for ln in last_names:
                    yield f"{fn}.{ln}".lower()

        name_iter = candidate_names()

        def next_unique_name():
            for cand in name_iter:
                if cand not in used_names:
                    used_names.add(cand)
                    return cand
            idx = len(used_names) + 1
            while True:
                cand = f"viewer{idx:05d}"
                if cand not in used_names:
                    used_names.add(cand)
                    return cand
                idx += 1

        renamed_accounts = []
        for acc in accounts:
            if looks_generic(acc.username):
                acc.username = next_unique_name()
                renamed_accounts.append(acc)

        max_user_id = max([acc.user_id for acc in accounts], default=0)
        created_accounts = []
        current_count = len(accounts)
        while current_count + len(created_accounts) < required:
            max_user_id += 1
            created_accounts.append(
                UserAccount(
                    username=next_unique_name(),
                    password_hash="",
                    user_id=max_user_id,
                )
            )

        if not dry_run:
            if renamed_accounts:
                UserAccount.objects.bulk_update(renamed_accounts, ["username"])
            if created_accounts:
                UserAccount.objects.bulk_create(created_accounts, batch_size=500)

            all_user_ids = list(UserAccount.objects.values_list("user_id", flat=True))
            existing_profiles = set(UserProfile.objects.values_list("user_id", flat=True))
            missing_profiles = [
                UserProfile(user_id=uid)
                for uid in all_user_ids
                if uid not in existing_profiles
            ]
            if missing_profiles:
                UserProfile.objects.bulk_create(missing_profiles, batch_size=1000)

        user_map = dict(UserAccount.objects.values_list("user_id", "username"))
        user_ids = sorted(user_map.keys())

        return {
            "user_ids": user_ids,
            "user_map": user_map,
            "renamed": len(renamed_accounts),
            "created": len(created_accounts),
        }

    @staticmethod
    def _write_missing_report(path: Path, rows):
        lines = [
            "# App Movies Without Reviews",
            "",
            f"Total movies without reviews: {len(rows)}",
            "",
        ]
        for title, year in rows:
            title_text = clean_text(title)
            if year:
                lines.append(f"- {title_text} ({int(year)})")
            else:
                lines.append(f"- {title_text}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def handle(self, *args, **options):
        target_movies = max(1000, int(options["target_movies"]))
        min_review_length = max(5, int(options["min_review_length"]))
        min_self_users = max(2, int(options["min_self_users"]))
        batch_size = max(500, int(options["batch_size"]))
        dry_run = bool(options["dry_run"])
        rng = random.Random(int(options["seed"]))

        movies_file = Path(options["movies_file"]).resolve()
        fallback_movies_file = Path(options["fallback_movies_file"]).resolve()
        missing_output = Path(options["missing_output"]).resolve()

        self.stdout.write("Step 1/8: Loading movie source...")
        rows, source_path = self._load_movie_rows(movies_file, fallback_movies_file, target_movies)
        self.stdout.write(f"  Source: {source_path}")
        self.stdout.write(f"  Movies selected: {len(rows):,}")

        self.stdout.write("Step 2/8: Syncing movie catalog...")
        sync_stats = self._sync_movies(rows, batch_size=batch_size, dry_run=dry_run)
        self.stdout.write(
            f"  Created: {sync_stats['created']:,}, Updated: {sync_stats['updated']:,}, Removed: {sync_stats['removed']:,}"
        )

        self.stdout.write("Step 3/8: Parsing all JSON review files...")
        review_files = self._gather_review_files(options["reviews_glob"])
        self.stdout.write(f"  Review files matched: {len(review_files):,}")
        for rf in review_files:
            self.stdout.write(f"    - {rf}")

        parsed = self._parse_reviews(
            review_files=review_files,
            target_rows=rows,
            min_review_length=min_review_length,
        )
        movie_reviews = parsed["movie_reviews"]
        movie_rating_hint = parsed["movie_rating_hint"]
        self.stdout.write(f"  Parsed JSON lines: {parsed['parsed_lines']:,}")
        self.stdout.write(f"  Matched lines to catalog: {parsed['matched_lines']:,}")
        self.stdout.write(f"  IMDb-id direct matches: {parsed['imdb_matches']:,}")
        self.stdout.write(f"  Movies with reviews from source: {len(movie_reviews):,}")
        self.stdout.write(f"  Max reviews for a single movie: {parsed['max_reviews_per_movie']:,}")

        self.stdout.write("Step 4/8: Ensuring real self users...")
        required_users = max(min_self_users, parsed["max_reviews_per_movie"])
        users_stats = self._ensure_real_users(required_users, dry_run=dry_run)
        self_users = users_stats["user_ids"]
        self.stdout.write(
            f"  Self users available: {len(self_users):,} (renamed: {users_stats['renamed']:,}, created: {users_stats['created']:,})"
        )

        self.stdout.write("Step 5/8: Removing legacy non-self-user rows...")
        legacy_counts = {}
        for model in (Rating, Review, Watchlist, WatchedMovie, UserProfile):
            qs = model.objects.exclude(user_id__in=self_users)
            legacy_counts[model.__name__] = qs.count()
            if not dry_run:
                qs.delete()
        self.stdout.write(f"  Legacy rows removed: {legacy_counts}")

        target_set = sync_stats["target_set"]
        target_ids = sync_stats["target_ids"]

        ratings_qs = Rating.objects.filter(user_id__in=self_users)
        if len(target_set) <= self.DB_IN_CHUNK:
            ratings_qs = ratings_qs.filter(movie_id__in=target_set)

        existing_self_ratings = {
            (int(uid), int(mid)): float(val)
            for uid, mid, val in ratings_qs.values_list("user_id", "movie_id", "rating")
        }

        self.stdout.write("Step 6/8: Rebuilding reviews and ratings using all source reviews...")
        if not dry_run:
            review_qs = Review.objects.filter(user_id__in=self_users)
            rating_qs = Rating.objects.filter(user_id__in=self_users)
            if len(target_set) <= self.DB_IN_CHUNK:
                review_qs = review_qs.filter(movie_id__in=target_set)
                rating_qs = rating_qs.filter(movie_id__in=target_set)
            review_qs.delete()
            rating_qs.delete()

        review_objects = []
        rating_objects = []
        total_reviews = 0
        total_ratings = 0
        ts = int(time.time())

        shuffled_users = list(self_users)
        rng.shuffle(shuffled_users)

        for movie_id in target_ids:
            texts = movie_reviews.get(int(movie_id), [])
            if not texts:
                continue

            review_count = len(texts)
            if review_count > len(shuffled_users):
                raise CommandError(
                    f"Not enough self users ({len(shuffled_users)}) for {review_count} reviews on movie_id={movie_id}."
                )

            chosen_users = rng.sample(shuffled_users, k=review_count)

            for user_id, review_text in zip(chosen_users, texts):
                review_objects.append(
                    Review(
                        user_id=int(user_id),
                        movie_id=int(movie_id),
                        review_text=review_text,
                    )
                )
                total_reviews += 1

                rating_value = movie_rating_hint.get(int(movie_id))
                if rating_value is None:
                    rating_value = existing_self_ratings.get((int(user_id), int(movie_id)))

                if rating_value is not None:
                    rating_objects.append(
                        Rating(
                            user_id=int(user_id),
                            movie_id=int(movie_id),
                            rating=float(rating_value),
                            timestamp=ts,
                        )
                    )
                    total_ratings += 1

        if not dry_run:
            if review_objects:
                Review.objects.bulk_create(review_objects, batch_size=batch_size)
            if rating_objects:
                Rating.objects.bulk_create(rating_objects, batch_size=batch_size)

        self.stdout.write(f"  Reviews inserted: {total_reviews:,}")
        self.stdout.write(f"  Ratings inserted: {total_ratings:,}")

        self.stdout.write("Step 7/8: Writing missing-review movie report...")
        missing_rows = list(
            Movie.objects.annotate(review_count=Count("reviews"))
            .filter(review_count=0)
            .values_list("title", "year")
            .order_by("-year", "title")
        )
        if not dry_run:
            self._write_missing_report(missing_output, missing_rows)
        self.stdout.write(f"  Movies without reviews: {len(missing_rows):,}")
        self.stdout.write(f"  Report file: {missing_output}")

        self.stdout.write("Step 8/8: Clearing cache...")
        if not dry_run:
            cache.clear()

        self.stdout.write(self.style.SUCCESS("\n✅ Full catalog + all-source review restore completed."))
        self.stdout.write(f"  Final movie target: {len(target_set):,}")
        self.stdout.write(f"  Final self users: {len(self_users):,}")
        if dry_run:
            self.stdout.write("  Mode: dry-run (no writes)")