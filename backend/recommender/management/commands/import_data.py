"""
Django management command to import processed CSV data into the database.
Usage: python manage.py import_data
"""

import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings
from recommender.models import Movie, Rating


class Command(BaseCommand):
    help = "Import processed movie and rating data into the database."

    @staticmethod
    def _normalize_image_path(raw_value):
        path = str(raw_value or "").strip()
        if not path:
            return ""

        lower_path = path.lower()
        if lower_path in {"nan", "none", "null"}:
            return ""

        if lower_path.startswith(("http://", "https://")):
            return path

        if path.startswith("images/"):
            path = f"/{path}"

        if path.startswith("/images/"):
            return f"https://m.media-amazon.com{path}"

        return path

    def add_arguments(self, parser):
        parser.add_argument('--movies-file', type=str, default=None,
                            help='Path to movies CSV (defaults to project root movies_merged.csv if present)')
        parser.add_argument('--ratings-file', type=str, default=None,
                            help='Path to ratings CSV (defaults to project root ratings_subset.csv if present)')
        parser.add_argument('--min-year', type=int, default=1990,
                            help='Only import movies with year >= min-year (default: 1990)')
        parser.add_argument('--require-images', action='store_true',
                            help='Import only movies with poster/backdrop images')
        parser.add_argument('--movies-only', action='store_true',
                            help='Import only movies, skip ratings')
        parser.add_argument('--ratings-limit', type=int, default=300000,
                    help='Max ratings to import (default: 300000)')

    def handle(self, *args, **options):
        data_dir = settings.DATA_DIR
        project_root = settings.PROJECT_ROOT
        movies_only = options['movies_only']
        ratings_limit = options['ratings_limit']
        min_year = options['min_year']
        require_images = options['require_images']

        default_movies_sources = [
            os.path.join(project_root, "movies_merged.csv"),
            os.path.join(data_dir, "movies_merged.csv"),
        ]
        default_ratings_sources = [
            os.path.join(project_root, "ratings_subset.csv"),
            os.path.join(data_dir, "ratings_subset.csv"),
        ]

        # ── Import Movies ──
        if options.get('movies_file'):
            movie_sources = [options['movies_file']]
        else:
            movie_sources = [path for path in default_movies_sources if os.path.exists(path)]

        if not movie_sources:
            raise FileNotFoundError("No movies CSV source found for import_data")

        if len(movie_sources) == 1:
            self.stdout.write(f"Loading movies from {movie_sources[0]}...")
            movies_df = pd.read_csv(movie_sources[0]).fillna("")
        else:
            self.stdout.write("Loading and merging movies from sources (priority order):")
            movie_frames = []
            for priority, source_path in enumerate(movie_sources):
                self.stdout.write(f"  - {source_path}")
                source_df = pd.read_csv(source_path).fillna("")
                movie_frames.append(source_df.assign(_source_priority=priority))

            movies_df = pd.concat(movie_frames, ignore_index=True)
            movies_df = movies_df.sort_values("_source_priority")
            movies_df = movies_df.drop_duplicates(subset=["movieId"], keep="first")

        movies_df["year_numeric"] = pd.to_numeric(movies_df.get("year"), errors="coerce")
        if min_year and int(min_year) > 0:
            before_year_filter = len(movies_df)
            movies_df = movies_df[movies_df["year_numeric"] >= int(min_year)]
            removed_by_year = before_year_filter - len(movies_df)
            if removed_by_year > 0:
                self.stdout.write(
                    f"  Skipped {removed_by_year:,} movies older than {int(min_year)}"
                )

        movies_df["poster_path"] = movies_df["poster_path"].astype(str).str.strip()
        movies_df["backdrop_path"] = movies_df["backdrop_path"].astype(str).str.strip()

        if require_images:
            before_count = len(movies_df)
            movies_df = movies_df[
                (movies_df["poster_path"] != "")
                | (movies_df["backdrop_path"] != "")
            ]
            removed_no_image = before_count - len(movies_df)
            if removed_no_image > 0:
                self.stdout.write(
                    f"  Skipped {removed_no_image:,} movies without poster/backdrop images"
                )

        # Clear existing
        Movie.objects.all().delete()

        batch = []
        for _, row in movies_df.iterrows():
            tmdb_value = pd.to_numeric(row.get("tmdbId", ""), errors="coerce")
            popularity_value = pd.to_numeric(row.get("popularity", 0), errors="coerce")
            vote_average_value = pd.to_numeric(row.get("vote_average", 0), errors="coerce")
            poster_path = self._normalize_image_path(row.get("poster_path", ""))
            backdrop_path = self._normalize_image_path(row.get("backdrop_path", ""))
            batch.append(Movie(
                movie_id=int(row["movieId"]),
                tmdb_id=int(tmdb_value) if pd.notna(tmdb_value) else None,
                title=str(row["title"]),
                year=int(row["year_numeric"]) if pd.notna(row["year_numeric"]) else None,
                genres=str(row.get("genres", "")),
                overview=str(row.get("overview", "")),
                original_language=str(row.get("original_language", "en")),
                popularity=float(popularity_value) if pd.notna(popularity_value) else 0.0,
                vote_average=float(vote_average_value) if pd.notna(vote_average_value) else 0.0,
                poster_path=poster_path,
                backdrop_path=backdrop_path,
                keywords=str(row.get("keywords", "")),
                release_date=str(row.get("release_date", "")),
                tagline=str(row.get("tagline", "")),
            ))

            if len(batch) >= 5000:
                Movie.objects.bulk_create(batch, ignore_conflicts=True)
                self.stdout.write(f"  Imported {Movie.objects.count()} movies...")
                batch = []

        if batch:
            Movie.objects.bulk_create(batch, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(
            f"✅ Imported {Movie.objects.count()} movies"
        ))

        if movies_only:
            return

        # ── Import Ratings ──
        if options.get('ratings_file'):
            ratings_sources = [options['ratings_file']]
        else:
            ratings_sources = [path for path in default_ratings_sources if os.path.exists(path)]

        if not ratings_sources:
            raise FileNotFoundError("No ratings CSV source found for import_data")

        self.stdout.write("\nLoading ratings from sources (priority order):")
        rating_frames = []
        for priority, source_path in enumerate(ratings_sources):
            self.stdout.write(f"  - {source_path}")
            rating_frames.append(pd.read_csv(source_path).assign(_source_priority=priority))

        ratings_df = pd.concat(rating_frames, ignore_index=True)
        ratings_df = ratings_df.sort_values("_source_priority")
        ratings_df = ratings_df.drop_duplicates(subset=["userId", "movieId"], keep="first")
        if ratings_limit and int(ratings_limit) > 0:
            ratings_df = ratings_df.head(int(ratings_limit))
        self.stdout.write(f"  Limit: {ratings_limit:,} ratings")

        # Filter to valid movies
        valid_movie_ids = set(Movie.objects.values_list('movie_id', flat=True))
        ratings_df = ratings_df[ratings_df["movieId"].isin(valid_movie_ids)]

        Rating.objects.all().delete()

        batch = []
        count = 0
        for _, row in ratings_df.iterrows():
            timestamp_value = pd.to_numeric(row.get("timestamp", ""), errors="coerce")
            batch.append(Rating(
                user_id=int(row["userId"]),
                movie_id=int(row["movieId"]),
                rating=float(row["rating"]),
                timestamp=int(timestamp_value) if pd.notna(timestamp_value) else None,
            ))
            count += 1

            if len(batch) >= 10000:
                Rating.objects.bulk_create(batch, ignore_conflicts=True)
                self.stdout.write(f"  Imported {count:,} ratings...")
                batch = []

        if batch:
            Rating.objects.bulk_create(batch, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(
            f"✅ Imported {Rating.objects.count()} ratings from {ratings_df['userId'].nunique()} users"
        ))
