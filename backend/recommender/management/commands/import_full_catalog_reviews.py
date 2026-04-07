"""
Restore a large movie catalog from CSV and bulk import multi-review data from JSONL.

Key behavior:
- Keeps approximately 26k movies (or fewer if CSV has less).
- Uses only self-account users (UserAccount) for review/rating ownership.
- Renames generic test usernames to realistic names and creates extra users if needed.
- Assigns 2-10 reviews per movie (configurable), reusing review text when required.
- Uses source rating if available; otherwise keeps prior self-user rating for the pair.
- Writes movies with no reviews to movie.md.

This command is optimized for speed by using bulk create/update and batched deletes.
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


def normalize_title(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", text).strip()


def strip_trailing_year(value: str) -> str:
    return re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", value or "").strip()


def parse_year_from_title(title: str):
    match = re.search(r"\((19\d{2}|20\d{2})\)\s*$", title or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


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
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def extract_rating(record: dict):
    # Accept common fields if present in source.
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
    help = "Import full movie catalog and fast multi-review assignment from JSONL."

    def add_arguments(self, parser):
        parser.add_argument(
            "--movies-csv",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "movies.csv"),
            help="Path to movies.csv (MovieLens style).",
        )
        parser.add_argument(
            "--reviews-file",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "movie_reviews_all_10_raw.jsonl"),
            help="Path to JSONL review file.",
        )
        parser.add_argument(
            "--target-movies",
            type=int,
            default=26000,
            help="Number of movies to keep from CSV (default: 26000).",
        )
        parser.add_argument(
            "--min-reviews",
            type=int,
            default=2,
            help="Minimum reviews per movie (default: 2).",
        )
        parser.add_argument(
            "--max-reviews",
            type=int,
            default=10,
            help="Maximum reviews per movie (default: 10).",
        )
        parser.add_argument(
            "--min-review-length",
            type=int,
            default=20,
            help="Minimum review text length to accept.",
        )
        parser.add_argument(
            "--min-self-users",
            type=int,
            default=24,
            help="Ensure at least this many self users for diversified review ownership.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=8000,
            help="Bulk insert batch size.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed.",
        )
        parser.add_argument(
            "--missing-output",
            type=str,
            default=str(Path(settings.PROJECT_ROOT) / "movie.md"),
            help="Output file for movies without reviews.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print planned actions without writing DB/file changes.",
        )

    @staticmethod
    def _write_missing_report(path: Path, rows):
        lines = [
            "# App Movies Without Reviews",
            "",
            f"Total movies without reviews: {len(rows)}",
            "",
        ]
        for title, year in rows:
            if year:
                lines.append(f"- {title} ({year})")
            else:
                lines.append(f"- {title}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _load_movies_csv(self, csv_path: Path, target_movies: int):
        if not csv_path.exists():
            raise CommandError(f"movies CSV not found: {csv_path}")

        df = pd.read_csv(csv_path, usecols=["movieId", "title", "genres"])
        if df.empty:
            raise CommandError("movies CSV is empty")

        df = df.dropna(subset=["movieId", "title"])
        df["movieId"] = pd.to_numeric(df["movieId"], errors="coerce")
        df = df.dropna(subset=["movieId"])\
            .drop_duplicates(subset=["movieId"], keep="first")

        df["movieId"] = df["movieId"].astype(int)
        df = df.sort_values("movieId")

        if target_movies > 0 and len(df) > target_movies:
            df = df.head(target_movies)

        rows = []
        for item in df.to_dict("records"):
            title = str(item.get("title") or "").strip()
            genres = str(item.get("genres") or "").strip()
            rows.append(
                {
                    "movie_id": int(item["movieId"]),
                    "title": title,
                    "genres": "" if genres == "(no genres listed)" else genres,
                    "year": parse_year_from_title(title),
                }
            )
        return rows

    def _sync_movies(self, csv_rows, batch_size: int, dry_run: bool):
        target_ids = [row["movie_id"] for row in csv_rows]
        target_set = set(target_ids)

        existing = {
            obj.movie_id: obj
            for obj in Movie.objects.filter(movie_id__in=target_set)
        }

        to_create = []
        to_update = []
        update_fields = ["title", "year", "genres"]

        for row in csv_rows:
            movie_id = row["movie_id"]
            title = row["title"]
            year = row["year"]
            genres = row["genres"]

            obj = existing.get(movie_id)
            if obj is None:
                to_create.append(
                    Movie(
                        movie_id=movie_id,
                        title=title,
                        year=year,
                        genres=genres,
                        tmdb_id=None,
                        overview="",
                        original_language="en",
                        popularity=0.0,
                        vote_average=0.0,
                        poster_path="",
                        backdrop_path="",
                        keywords="",
                        release_date="",
                        tagline="",
                    )
                )
                continue

            changed = False
            if title and obj.title != title:
                obj.title = title
                changed = True
            if year and obj.year != year:
                obj.year = year
                changed = True
            if genres and obj.genres != genres:
                obj.genres = genres
                changed = True
            if changed:
                to_update.append(obj)

        remove_qs = Movie.objects.exclude(movie_id__in=target_set)
        remove_count = remove_qs.count()

        if not dry_run:
            if to_create:
                Movie.objects.bulk_create(to_create, batch_size=batch_size, ignore_conflicts=True)
            if to_update:
                Movie.objects.bulk_update(to_update, update_fields, batch_size=batch_size)
            if remove_count:
                remove_qs.delete()

        return {
            "target_count": len(target_ids),
            "created": len(to_create),
            "updated": len(to_update),
            "removed": remove_count,
            "target_ids": target_ids,
            "target_set": target_set,
        }

    def _ensure_real_users(self, min_self_users: int, max_reviews: int, dry_run: bool):
        required = max(int(min_self_users), int(max_reviews), 2)

        first_names = [
            "Aarav", "Vivaan", "Aditya", "Arjun", "Ishaan", "Kabir", "Reyansh", "Vihaan",
            "Anaya", "Diya", "Ira", "Kiara", "Mira", "Aisha", "Myra", "Siya",
            "Rahul", "Rohan", "Vikram", "Aman", "Karan", "Neha", "Pooja", "Sneha",
        ]
        last_names = [
            "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kapoor", "Mehta", "Nair",
            "Reddy", "Iyer", "Malhotra", "Bose", "Joshi", "Kulkarni", "Chawla", "Saxena",
        ]

        accounts = list(UserAccount.objects.order_by("user_id"))
        used_names = {acc.username.lower() for acc in accounts}

        def name_candidates():
            for f in first_names:
                for l in last_names:
                    yield f"{f}.{l}".lower()

        pool_iter = name_candidates()

        def next_unique_name():
            for cand in pool_iter:
                if cand not in used_names:
                    used_names.add(cand)
                    return cand
            idx = len(used_names) + 1
            while True:
                cand = f"member{idx:04d}"
                if cand not in used_names:
                    used_names.add(cand)
                    return cand
                idx += 1

        def looks_generic(username: str):
            name = (username or "").lower()
            return (
                name.startswith("authtest")
                or name.startswith("pref_")
                or name.startswith("test")
                or bool(re.fullmatch(r"user\d+", name))
            )

        renamed = 0
        for acc in accounts:
            if not looks_generic(acc.username):
                continue
            new_name = next_unique_name()
            if acc.username == new_name:
                continue
            acc.username = new_name
            renamed += 1

        max_user_id = max([acc.user_id for acc in accounts], default=0)
        created_accounts = []
        while len(accounts) + len(created_accounts) < required:
            max_user_id += 1
            created_accounts.append(
                UserAccount(
                    username=next_unique_name(),
                    password_hash="",
                    user_id=max_user_id,
                )
            )

        if not dry_run:
            if renamed:
                UserAccount.objects.bulk_update([a for a in accounts if looks_generic(a.username) is False], ["username"])
            if created_accounts:
                UserAccount.objects.bulk_create(created_accounts, batch_size=200)
            all_user_ids = list(UserAccount.objects.values_list("user_id", flat=True))
            existing_profiles = set(UserProfile.objects.values_list("user_id", flat=True))
            missing_profiles = [
                UserProfile(user_id=uid)
                for uid in all_user_ids
                if uid not in existing_profiles
            ]
            if missing_profiles:
                UserProfile.objects.bulk_create(missing_profiles, batch_size=500)

        # Re-read users from DB to avoid stale state.
        user_map = dict(UserAccount.objects.values_list("user_id", "username"))
        user_ids = sorted(user_map.keys())
        return {
            "user_ids": user_ids,
            "user_map": user_map,
            "renamed": renamed,
            "created": len(created_accounts),
        }

    def _parse_reviews(self, reviews_file: Path, target_movie_rows, min_review_length: int):
        if not reviews_file.exists():
            raise CommandError(f"review file not found: {reviews_file}")

        exact_key_map = {}
        normalized_key_map = {}
        normalized_title_map = {}
        imdb_movie_id_map = {}
        for row in target_movie_rows:
            movie_id = int(row["movie_id"])
            title = str(row["title"] or "").strip()
            year = row.get("year")
            title_wo_year = strip_trailing_year(title)

            # Some catalogs store IMDb numeric id as movie_id.
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
        fallback_pool = []
        fallback_cap = 80000
        parsed_lines = 0
        matched_lines = 0
        imdb_matches = 0

        with reviews_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                parsed_lines += 1

                try:
                    rec = json.loads(text)
                except json.JSONDecodeError:
                    continue

                review_list = rec.get("reviews")
                if not isinstance(review_list, list):
                    review_list = []

                cleaned = []
                for item in review_list:
                    rv = str(item or "").replace("\x00", " ").strip()
                    if len(rv) >= min_review_length:
                        cleaned.append(rv[:2000])

                if cleaned and len(fallback_pool) < fallback_cap:
                    room = fallback_cap - len(fallback_pool)
                    fallback_pool.extend(cleaned[:room])

                movie_id = None

                imdb_raw = str(rec.get("imdb_id") or "").strip().lower()
                if imdb_raw:
                    imdb_digits = re.sub(r"[^0-9]", "", imdb_raw)
                    try:
                        imdb_numeric = int(imdb_digits) if imdb_digits else None
                    except (TypeError, ValueError):
                        imdb_numeric = None
                    if imdb_numeric is not None:
                        movie_id = imdb_movie_id_map.get(imdb_numeric)
                        if movie_id is not None:
                            imdb_matches += 1

                if movie_id is None:
                    movie_key = str(rec.get("movie_key") or "").strip()
                    if movie_key and movie_key in exact_key_map:
                        movie_id = exact_key_map[movie_key]
                    elif movie_key and "|" in movie_key:
                        left, right = movie_key.rsplit("|", 1)
                        norm = normalize_title(strip_trailing_year(left))
                        try:
                            year_int = int(right)
                        except (TypeError, ValueError):
                            year_int = None
                        if year_int is not None:
                            movie_id = normalized_key_map.get((norm, year_int))
                        if movie_id is None:
                            movie_id = normalized_title_map.get(norm)

                if movie_id is None:
                    title = str(rec.get("query_title") or rec.get("matched_title") or "").strip()
                    if title:
                        title_norm = normalize_title(strip_trailing_year(title))
                        year = rec.get("query_year") or rec.get("matched_year")
                        try:
                            year_int = int(year) if year is not None else None
                        except (TypeError, ValueError):
                            year_int = None
                        if year_int is not None:
                            movie_id = normalized_key_map.get((title_norm, year_int))
                        if movie_id is None:
                            movie_id = normalized_title_map.get(title_norm)

                if movie_id is None:
                    continue

                matched_lines += 1
                if cleaned:
                    movie_reviews[int(movie_id)].extend(cleaned)

                hint = extract_rating(rec)
                if hint is not None and int(movie_id) not in movie_rating_hint:
                    movie_rating_hint[int(movie_id)] = hint

        return {
            "movie_reviews": movie_reviews,
            "movie_rating_hint": movie_rating_hint,
            "fallback_pool": fallback_pool,
            "parsed_lines": parsed_lines,
            "matched_lines": matched_lines,
            "imdb_matches": imdb_matches,
        }

    @staticmethod
    def _select_review_texts(texts, fallback_pool, count: int, rng):
        unique = list(dict.fromkeys(texts))
        out = []

        if unique:
            if len(unique) >= count:
                return rng.sample(unique, k=count)
            out.extend(unique)

        source = unique if unique else fallback_pool
        if not source:
            return out

        while len(out) < count:
            out.append(source[rng.randrange(len(source))])
        return out

    def handle(self, *args, **options):
        movies_csv = Path(options["movies_csv"]).resolve()
        reviews_file = Path(options["reviews_file"]).resolve()
        target_movies = max(1000, int(options["target_movies"]))
        min_reviews = max(1, int(options["min_reviews"]))
        max_reviews = max(min_reviews, int(options["max_reviews"]))
        min_review_length = max(5, int(options["min_review_length"]))
        min_self_users = max(2, int(options["min_self_users"]))
        batch_size = max(500, int(options["batch_size"]))
        dry_run = bool(options["dry_run"])
        missing_output = Path(options["missing_output"]).resolve()

        rng = random.Random(int(options["seed"]))

        self.stdout.write("Step 1/8: Loading target movie catalog from CSV...")
        csv_rows = self._load_movies_csv(movies_csv, target_movies)
        self.stdout.write(f"  CSV movies selected: {len(csv_rows):,}")

        self.stdout.write("Step 2/8: Syncing movie catalog in DB...")
        sync_stats = self._sync_movies(csv_rows, batch_size=batch_size, dry_run=dry_run)
        self.stdout.write(
            f"  Created: {sync_stats['created']:,}, Updated: {sync_stats['updated']:,}, Removed: {sync_stats['removed']:,}"
        )

        self.stdout.write("Step 3/8: Ensuring real self-user accounts...")
        users_stats = self._ensure_real_users(
            min_self_users=min_self_users,
            max_reviews=max_reviews,
            dry_run=dry_run,
        )
        self_users = users_stats["user_ids"]
        self.stdout.write(
            f"  Self users: {len(self_users):,} (renamed: {users_stats['renamed']:,}, created: {users_stats['created']:,})"
        )

        self.stdout.write("Step 4/8: Removing legacy non-self user data...")
        legacy_counts = {}
        for model in (Rating, Review, Watchlist, WatchedMovie, UserProfile):
            qs = model.objects.exclude(user_id__in=self_users)
            legacy_counts[model.__name__] = qs.count()
            if not dry_run:
                qs.delete()
        self.stdout.write(f"  Legacy rows removed: {legacy_counts}")

        self.stdout.write("Step 5/8: Parsing JSONL reviews and matching movies...")
        parsed = self._parse_reviews(
            reviews_file=reviews_file,
            target_movie_rows=csv_rows,
            min_review_length=min_review_length,
        )
        movie_reviews = parsed["movie_reviews"]
        movie_rating_hint = parsed["movie_rating_hint"]
        fallback_pool = parsed["fallback_pool"]

        self.stdout.write(f"  Parsed lines: {parsed['parsed_lines']:,}")
        self.stdout.write(f"  Matched lines: {parsed['matched_lines']:,}")
        self.stdout.write(f"  IMDb-id direct matches: {parsed['imdb_matches']:,}")
        self.stdout.write(f"  Movies with direct reviews: {len(movie_reviews):,}")
        self.stdout.write(f"  Fallback pool size: {len(fallback_pool):,}")

        if not movie_reviews and not fallback_pool:
            raise CommandError("No usable review text found in JSONL source.")

        target_ids = sync_stats["target_ids"]
        target_set = sync_stats["target_set"]

        existing_self_ratings = {
            (int(uid), int(mid)): float(val)
            for uid, mid, val in Rating.objects.filter(
                user_id__in=self_users,
                movie_id__in=target_set,
            ).values_list("user_id", "movie_id", "rating")
        }

        self.stdout.write("Step 6/8: Rebuilding reviews/ratings in bulk (fast path)...")
        if not dry_run:
            Review.objects.filter(user_id__in=self_users, movie_id__in=target_set).delete()
            Rating.objects.filter(user_id__in=self_users, movie_id__in=target_set).delete()

        review_objects = []
        rating_objects = []
        ts = int(time.time())
        total_reviews = 0
        total_ratings = 0

        # Shuffle once so ownership and ordering are not repetitive.
        shuffled_users = list(self_users)
        rng.shuffle(shuffled_users)

        for movie_id in target_ids:
            desired = rng.randint(min_reviews, max_reviews)
            desired = min(desired, len(shuffled_users))
            if desired <= 0:
                continue

            texts = self._select_review_texts(
                texts=movie_reviews.get(int(movie_id), []),
                fallback_pool=fallback_pool,
                count=desired,
                rng=rng,
            )
            if not texts:
                continue

            # Distinct users per movie to satisfy unique(user_id, movie_id).
            chosen_users = rng.sample(shuffled_users, k=desired)

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

        self.stdout.write("Step 7/8: Writing movies without reviews report...")
        missing_rows = list(
            Movie.objects.annotate(review_count=Count("reviews"))
            .filter(review_count=0)
            .values_list("title", "year")
            .order_by("-year", "title")
        )

        if not dry_run:
            self._write_missing_report(missing_output, missing_rows)

        self.stdout.write(f"  Missing-review movies: {len(missing_rows):,}")
        self.stdout.write(f"  Report: {missing_output}")

        self.stdout.write("Step 8/8: Clearing cache...")
        if not dry_run:
            cache.clear()

        self.stdout.write(self.style.SUCCESS("\n✅ Full catalog + multi-review import completed."))
        self.stdout.write(f"  Final target movie set: {len(target_set):,}")
        self.stdout.write(f"  Self users available: {len(self_users):,}")
        if dry_run:
            self.stdout.write("  Mode: dry-run (no DB/file writes)")