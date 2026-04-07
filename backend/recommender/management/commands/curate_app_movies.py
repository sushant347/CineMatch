"""
Curate app movie catalog and import external review text from JSONL.

What this command does:
1) Keeps only self-account users' data (removes legacy/demo user rows).
2) Curates ~200+ movies (mostly newer, old movies only when highly rated).
3) Removes movies not used by app catalog/search.
4) Imports reviews from movie_reviews_all_10_raw.jsonl.
5) Uses rating from source when available; otherwise keeps existing self-user ratings.
6) Writes names of app movies without reviews into movie.md.
"""

import json
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError

from recommender.models import (
    Movie,
    Rating,
    Review,
    UserAccount,
    UserProfile,
    Watchlist,
    WatchedMovie,
)


TARGET_TERMS = ("comedy", "animation", "crime", "thriller", "action", "horror", "mystery")


@dataclass
class MovieRow:
    movie_id: int
    title: str
    year: int | None
    genres: str
    original_language: str
    popularity: float
    vote_average: float
    terms: set

    @property
    def year_value(self):
        return int(self.year or 0)


def _normalize_title(title: str) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", (title or "").lower())
    return re.sub(r"\s+", " ", value).strip()


def _strip_trailing_year(title: str) -> str:
    return re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", title or "").strip()


def _parse_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None

    # Handle patterns like "8/10" or "4.5 stars".
    if "/" in text:
        num = text.split("/", 1)[0].strip()
        try:
            return float(num)
        except ValueError:
            pass

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _extract_rating(record: dict):
    rating_candidates = [
        "rating",
        "imdb_rating",
        "score",
        "stars",
        "vote_average",
    ]

    raw = None
    for key in rating_candidates:
        if key in record:
            raw = record.get(key)
            break

    if raw is None:
        return None

    value = _parse_float(raw)
    if value is None:
        return None

    # Convert from /10 scale when needed.
    if value > 5.0:
        value = value / 2.0

    value = max(0.5, min(5.0, value))
    value = round(value * 2.0) / 2.0
    return float(value)


class Command(BaseCommand):
    help = "Curate app movies, import JSONL reviews, prune unused movies, and write missing-review movie list."

    def add_arguments(self, parser):
        parser.add_argument(
            "--reviews-file",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "movie_reviews_all_10_raw.jsonl"),
            help="Path to review JSONL file.",
        )
        parser.add_argument(
            "--target-movies",
            type=int,
            default=240,
            help="Approximate number of movies to keep in app DB (default: 240).",
        )
        parser.add_argument(
            "--old-year-cutoff",
            type=int,
            default=2012,
            help="Movies older than this year are treated as old.",
        )
        parser.add_argument(
            "--old-min-rating",
            type=float,
            default=7.2,
            help="Minimum vote_average to keep old movies.",
        )
        parser.add_argument(
            "--reviews-per-movie",
            type=int,
            default=1,
            help="How many reviews to assign per kept movie (bounded by self-user count).",
        )
        parser.add_argument(
            "--missing-output",
            type=str,
            default=str(Path(settings.PROJECT_ROOT) / "movie.md"),
            help="Output file for app movie names with no reviews.",
        )
        parser.add_argument(
            "--min-review-length",
            type=int,
            default=20,
            help="Minimum review text length.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for deterministic split/assignment.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show actions without writing DB/file changes.",
        )

    @staticmethod
    def _build_movie_rows():
        rows = []
        for item in Movie.objects.values(
            "movie_id",
            "title",
            "year",
            "genres",
            "original_language",
            "popularity",
            "vote_average",
        ):
            genres = str(item.get("genres") or "")
            genres_l = genres.lower()
            terms = {term for term in TARGET_TERMS if term in genres_l}

            rows.append(
                MovieRow(
                    movie_id=int(item["movie_id"]),
                    title=str(item.get("title") or ""),
                    year=item.get("year"),
                    genres=genres,
                    original_language=str(item.get("original_language") or "").lower(),
                    popularity=float(item.get("popularity") or 0.0),
                    vote_average=float(item.get("vote_average") or 0.0),
                    terms=terms,
                )
            )
        return rows

    @staticmethod
    def _sort_new_then_pop(rows, old_year_cutoff):
        return sorted(
            rows,
            key=lambda m: (
                1 if m.year_value >= old_year_cutoff else 0,
                m.year_value,
                m.popularity,
                m.vote_average,
                m.movie_id,
            ),
            reverse=True,
        )

    @staticmethod
    def _sort_pop(rows):
        return sorted(
            rows,
            key=lambda m: (m.popularity, m.vote_average, m.year_value, m.movie_id),
            reverse=True,
        )

    def _pick_catalog(self, movies, target_movies, old_year_cutoff, old_min_rating):
        eligible = [
            m for m in movies
            if (m.year_value >= old_year_cutoff) or (m.vote_average >= old_min_rating)
        ]
        if not eligible:
            raise CommandError("No eligible movies found for curation.")

        selected_ids = []
        selected_set = set()

        def add_from(name, candidates, limit):
            added = 0
            for movie in candidates:
                if movie.movie_id in selected_set:
                    continue
                selected_ids.append(movie.movie_id)
                selected_set.add(movie.movie_id)
                added += 1
                if added >= limit:
                    break
            self.stdout.write(f"  {name}: +{added}")

        base_quotas = {
            "hindi": 24,
            "comedy": 34,
            "anime": 20,
            "crime_thriller": 34,
            "action_horror_mystery": 34,
            "trending": 70,
            "old_high": 24,
        }
        scale = float(target_movies) / 240.0
        quotas = {k: max(6, int(round(v * scale))) for k, v in base_quotas.items()}

        hindi_pool = self._sort_new_then_pop(
            [
                m for m in eligible
                if m.original_language == "hi" and not (m.terms & {"comedy", "animation", "crime", "thriller", "action", "horror", "mystery"})
            ],
            old_year_cutoff,
        )

        comedy_pool = self._sort_new_then_pop(
            [m for m in eligible if "comedy" in m.terms and m.terms <= {"comedy"}],
            old_year_cutoff,
        )

        anime_pool = self._sort_new_then_pop(
            [
                m for m in eligible
                if m.original_language == "ja" and "animation" in m.terms and m.terms <= {"animation"}
            ],
            old_year_cutoff,
        )

        crime_thriller_pool = self._sort_new_then_pop(
            [
                m for m in eligible
                if (m.terms & {"crime", "thriller"}) and m.terms <= {"crime", "thriller"}
            ],
            old_year_cutoff,
        )

        ahm_pool = self._sort_new_then_pop(
            [
                m for m in eligible
                if (m.terms & {"action", "horror", "mystery"})
                and m.terms <= {"action", "horror", "mystery"}
            ],
            old_year_cutoff,
        )

        trending_pool = self._sort_pop(eligible)

        old_high_pool = self._sort_pop(
            [m for m in eligible if m.year_value < old_year_cutoff and m.vote_average >= old_min_rating]
        )

        add_from("Hindi", hindi_pool, quotas["hindi"])
        add_from("Comedy", comedy_pool, quotas["comedy"])
        add_from("Japanese Animation", anime_pool, quotas["anime"])
        add_from("Crime/Thriller", crime_thriller_pool, quotas["crime_thriller"])
        add_from("Action/Horror/Mystery", ahm_pool, quotas["action_horror_mystery"])
        add_from("Trending", trending_pool, quotas["trending"])
        add_from("Old high-rated", old_high_pool, quotas["old_high"])

        if len(selected_ids) < target_movies:
            filler = self._sort_new_then_pop(eligible, old_year_cutoff)
            add_from("Filler", filler, target_movies - len(selected_ids))

        return selected_ids[:target_movies]

    def _write_missing_reviews_report(self, output_path: Path, missing_movies):
        lines = [
            "# App Movies Without Reviews",
            "",
            f"Total movies without reviews: {len(missing_movies)}",
            "",
        ]

        for movie in sorted(missing_movies, key=lambda m: ((m.year or 0), m.title), reverse=True):
            if movie.year:
                lines.append(f"- {movie.title} ({movie.year})")
            else:
                lines.append(f"- {movie.title}")

        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def handle(self, *args, **options):
        reviews_file = Path(options["reviews_file"]).resolve()
        target_movies = max(200, int(options["target_movies"]))
        old_year_cutoff = int(options["old_year_cutoff"])
        old_min_rating = float(options["old_min_rating"])
        reviews_per_movie = max(1, int(options["reviews_per_movie"]))
        min_review_length = max(5, int(options["min_review_length"]))
        missing_output = Path(options["missing_output"]).resolve()
        dry_run = bool(options["dry_run"])

        rng = random.Random(int(options["seed"]))

        if not reviews_file.exists():
            raise CommandError(f"Review file not found: {reviews_file}")

        self_users = list(UserAccount.objects.values_list("user_id", flat=True))
        if not self_users:
            raise CommandError("No self account users found. Create at least one account first.")

        self.stdout.write("Step 1/6: Removing legacy user data (keeping self-account users only)...")
        legacy_delete_counts = {}
        for model in (Rating, Review, Watchlist, WatchedMovie, UserProfile):
            qs = model.objects.exclude(user_id__in=self_users)
            legacy_delete_counts[model.__name__] = qs.count()
            if not dry_run:
                qs.delete()
        self.stdout.write(f"  Removed legacy rows: {legacy_delete_counts}")

        self.stdout.write("Step 2/6: Building curated app movie catalog...")
        all_movies = self._build_movie_rows()
        selected_ids = self._pick_catalog(all_movies, target_movies, old_year_cutoff, old_min_rating)
        selected_set = set(selected_ids)
        self.stdout.write(f"  Selected movies: {len(selected_set)}")

        movies_to_remove_qs = Movie.objects.exclude(movie_id__in=selected_set)
        remove_count = movies_to_remove_qs.count()
        self.stdout.write(f"  Movies to remove as unused: {remove_count}")
        if not dry_run:
            movies_to_remove_qs.delete()

        kept_movies = list(
            Movie.objects.filter(movie_id__in=selected_set).values(
                "movie_id", "title", "year", "vote_average", "popularity", "genres", "original_language"
            )
        )
        kept_by_id = {int(m["movie_id"]): m for m in kept_movies}

        exact_key_to_movie_id = {}
        normalized_key_to_movie_id = {}
        normalized_title_only_to_movie_id = {}
        imdb_movie_id_map = {}
        for m in kept_movies:
            movie_id = int(m["movie_id"])
            title = str(m.get("title") or "").strip()
            title_wo_year = _strip_trailing_year(title)
            year = m.get("year")

            # Some datasets encode IMDb numeric ids directly as movie_id.
            imdb_movie_id_map[movie_id] = movie_id

            norm_variants = {
                _normalize_title(title),
                _normalize_title(title_wo_year),
            }
            norm_variants.discard("")

            if year:
                exact_key_to_movie_id[f"{title}|{int(year)}"] = movie_id
                exact_key_to_movie_id[f"{title_wo_year}|{int(year)}"] = movie_id
                for norm_title in norm_variants:
                    normalized_key_to_movie_id[(norm_title, int(year))] = movie_id

            for norm_title in norm_variants:
                normalized_title_only_to_movie_id[norm_title] = movie_id

        self.stdout.write("Step 3/6: Parsing review JSONL and matching to kept movies...")
        movie_reviews_map = defaultdict(list)
        movie_rating_hint = {}
        fallback_pool = []
        fallback_cap = 30000
        parsed_lines = 0
        imdb_matches = 0

        with reviews_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                parsed_lines += 1
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                reviews = rec.get("reviews") or []
                if not isinstance(reviews, list):
                    reviews = []

                cleaned_reviews = []
                for text in reviews:
                    txt = str(text or "").replace("\x00", " ").strip()
                    if len(txt) >= min_review_length:
                        cleaned_reviews.append(txt[:2000])

                if cleaned_reviews and len(fallback_pool) < fallback_cap:
                    space_left = fallback_cap - len(fallback_pool)
                    fallback_pool.extend(cleaned_reviews[:space_left])

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
                    if movie_key and movie_key in exact_key_to_movie_id:
                        movie_id = exact_key_to_movie_id[movie_key]
                    else:
                        if movie_key and "|" in movie_key:
                            mk_title, mk_year = movie_key.rsplit("|", 1)
                            mk_title_norm = _normalize_title(_strip_trailing_year(mk_title.strip()))
                            try:
                                mk_year_int = int(mk_year)
                            except (TypeError, ValueError):
                                mk_year_int = None

                            if mk_year_int is not None:
                                movie_id = normalized_key_to_movie_id.get((mk_title_norm, mk_year_int))
                            if movie_id is None:
                                movie_id = normalized_title_only_to_movie_id.get(mk_title_norm)

                if movie_id is None:
                    title = str(rec.get("query_title") or rec.get("matched_title") or "").strip()
                    year = rec.get("query_year") or rec.get("matched_year")
                    if title and year:
                        try:
                            title_norm = _normalize_title(_strip_trailing_year(title))
                            movie_id = normalized_key_to_movie_id.get((title_norm, int(year)))
                        except (TypeError, ValueError):
                            movie_id = None

                if movie_id is None:
                    title = str(rec.get("query_title") or rec.get("matched_title") or "").strip()
                    if title:
                        movie_id = normalized_title_only_to_movie_id.get(
                            _normalize_title(_strip_trailing_year(title))
                        )

                if movie_id is None:
                    continue

                if cleaned_reviews:
                    movie_reviews_map[movie_id].extend(cleaned_reviews)

                rating_hint = _extract_rating(rec)
                if rating_hint is not None and movie_id not in movie_rating_hint:
                    movie_rating_hint[movie_id] = rating_hint

        self.stdout.write(f"  Parsed lines: {parsed_lines:,}")
        self.stdout.write(f"  IMDb-id direct matches: {imdb_matches:,}")
        self.stdout.write(f"  Movies with matched reviews: {len(movie_reviews_map):,}")
        self.stdout.write(f"  Fallback review pool size: {len(fallback_pool):,}")

        if not fallback_pool and not movie_reviews_map:
            raise CommandError("No usable reviews found in source file.")

        self.stdout.write("Step 4/6: Writing reviews and ratings for kept movies...")
        existing_self_ratings = {
            (int(uid), int(mid)): float(val)
            for uid, mid, val in Rating.objects.filter(
                user_id__in=self_users,
                movie_id__in=selected_set,
            ).values_list("user_id", "movie_id", "rating")
        }

        now_ts = int(time.time())
        review_rows_written = 0
        rating_rows_written = 0

        for movie_id in selected_ids:
            texts = movie_reviews_map.get(movie_id, [])
            if not texts and fallback_pool:
                texts = [fallback_pool[rng.randrange(len(fallback_pool))]]

            if not texts:
                continue

            max_for_movie = min(reviews_per_movie, len(self_users))
            chosen_users = rng.sample(self_users, k=max_for_movie)

            for user_id in chosen_users:
                review_text = texts[rng.randrange(len(texts))]

                if not dry_run:
                    Review.objects.update_or_create(
                        user_id=int(user_id),
                        movie_id=int(movie_id),
                        defaults={"review_text": review_text},
                    )
                review_rows_written += 1

                rating_value = movie_rating_hint.get(movie_id)
                if rating_value is None:
                    rating_value = existing_self_ratings.get((int(user_id), int(movie_id)))

                if rating_value is not None:
                    if not dry_run:
                        Rating.objects.update_or_create(
                            user_id=int(user_id),
                            movie_id=int(movie_id),
                            defaults={"rating": float(rating_value), "timestamp": now_ts},
                        )
                    rating_rows_written += 1

        self.stdout.write(f"  Reviews written/updated: {review_rows_written:,}")
        self.stdout.write(f"  Ratings written/updated: {rating_rows_written:,}")

        self.stdout.write("Step 5/6: Writing movie names that still have no reviews...")
        reviewed_ids = set(
            Review.objects.filter(movie_id__in=selected_set).values_list("movie_id", flat=True)
        )
        missing_movies = [
            MovieRow(
                movie_id=int(m["movie_id"]),
                title=str(m.get("title") or ""),
                year=m.get("year"),
                genres=str(m.get("genres") or ""),
                original_language=str(m.get("original_language") or ""),
                popularity=float(m.get("popularity") or 0.0),
                vote_average=float(m.get("vote_average") or 0.0),
                terms=set(),
            )
            for m in kept_movies
            if int(m["movie_id"]) not in reviewed_ids
        ]

        if not dry_run:
            self._write_missing_reviews_report(missing_output, missing_movies)

        self.stdout.write(f"  Missing-review movie count: {len(missing_movies):,}")
        self.stdout.write(f"  Output file: {missing_output}")

        self.stdout.write("Step 6/6: Clearing cache...")
        if not dry_run:
            cache.clear()

        self.stdout.write(self.style.SUCCESS("\n✅ App movie curation + review sync completed."))
        self.stdout.write(f"  Kept movies: {len(selected_set):,}")
        self.stdout.write(f"  Removed movies: {remove_count:,}")
        self.stdout.write(f"  Self users kept: {len(self_users):,}")
        if dry_run:
            self.stdout.write("  Mode: dry-run (no DB/file writes)")