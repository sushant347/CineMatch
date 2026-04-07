"""
Fast image-path sync for movies using links.csv + TMDB movie dataset.

Purpose:
- Fill missing tmdb_id/poster_path/backdrop_path for movies already in DB.
- Keep processing fast via chunked CSV reads and bulk updates.
"""

from pathlib import Path
import re

import pandas as pd
from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError

from recommender.models import Movie


def _clean_path(value):
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return ""
    if not text.startswith("/"):
        return f"/{text}"
    return text


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _normalize_title(value):
    text = _clean_text(value).lower()
    if not text:
        return ""

    # Remove trailing year marker, e.g. "Movie Name (1995)".
    text = re.sub(r"\s*\(\d{4}\)\s*$", "", text)
    # Normalize punctuation/spaces.
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


def _parse_year_from_release_date(value):
    text = _clean_text(value)
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


class Command(BaseCommand):
    help = "Fill missing movie image links quickly from CSV sources."

    def add_arguments(self, parser):
        parser.add_argument(
            "--links-csv",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "links.csv"),
            help="Path to links.csv",
        )
        parser.add_argument(
            "--tmdb-csv",
            type=str,
            default=str(Path(settings.RAW_DATA_DIR) / "TMDB_movie_dataset_v11.csv"),
            help="Path to TMDB movie CSV",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Bulk update batch size",
        )
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=200000,
            help="TMDB CSV read chunk size",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Attempt enrichment for all movies, not only missing-image rows",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview only",
        )

    def handle(self, *args, **options):
        links_csv = Path(options["links_csv"]).resolve()
        tmdb_csv = Path(options["tmdb_csv"]).resolve()
        batch_size = max(500, int(options["batch_size"]))
        chunk_size = max(20000, int(options["chunk_size"]))
        all_rows = bool(options["all"])
        dry_run = bool(options["dry_run"])

        if not links_csv.exists():
            raise CommandError(f"links.csv not found: {links_csv}")
        if not tmdb_csv.exists():
            raise CommandError(f"TMDB CSV not found: {tmdb_csv}")

        movies = list(Movie.objects.all())
        if not movies:
            raise CommandError("No movies found in DB.")

        movie_map = {int(m.movie_id): m for m in movies}
        movie_ids = set(movie_map.keys())
        self.stdout.write(f"Movies in DB: {len(movie_ids):,}")

        self.stdout.write("Loading links mapping...")
        links_df = pd.read_csv(links_csv, usecols=["movieId", "tmdbId"])
        links_df["movieId"] = pd.to_numeric(links_df["movieId"], errors="coerce")
        links_df["tmdbId"] = pd.to_numeric(links_df["tmdbId"], errors="coerce")
        links_df = links_df.dropna(subset=["movieId", "tmdbId"])
        links_df["movieId"] = links_df["movieId"].astype(int)
        links_df["tmdbId"] = links_df["tmdbId"].astype(int)
        links_df = links_df[links_df["movieId"].isin(movie_ids)]
        link_map = dict(zip(links_df["movieId"], links_df["tmdbId"]))
        self.stdout.write(f"  Movie->TMDB links found: {len(link_map):,}")

        tmdb_ids_needed = set()
        linked_count = 0
        for movie_id, movie in movie_map.items():
            if not movie.tmdb_id:
                tmdb_id = link_map.get(movie_id)
                if tmdb_id:
                    movie.tmdb_id = int(tmdb_id)
                    linked_count += 1

            if not movie.tmdb_id:
                continue

            if all_rows or not (movie.poster_path or movie.backdrop_path):
                tmdb_ids_needed.add(int(movie.tmdb_id))

        self.stdout.write(f"  tmdb_id backfilled from links: {linked_count:,}")
        self.stdout.write(f"  TMDB ids requested for enrichment: {len(tmdb_ids_needed):,}")

        if not tmdb_ids_needed:
            if not dry_run:
                Movie.objects.bulk_update(list(movie_map.values()), ["tmdb_id"], batch_size=batch_size)
            self.stdout.write(self.style.SUCCESS("No image enrichment required."))
            return

        self.stdout.write("Reading TMDB dataset in chunks and collecting matches...")
        header_cols = set(pd.read_csv(tmdb_csv, nrows=0).columns)
        wanted_cols = [
            "id",
            "title",
            "original_title",
            "poster_path",
            "backdrop_path",
            "overview",
            "original_language",
            "popularity",
            "vote_average",
            "vote_count",
            "release_date",
            "tagline",
        ]
        usecols = [col for col in wanted_cols if col in header_cols]
        if "id" not in usecols:
            raise CommandError("TMDB CSV does not contain required 'id' column.")

        tmdb_map = {}

        # Prepare fallback lookup set for rows still missing both images after ID pass.
        title_year_needed = {}
        norm_title_needed = set()
        for movie in movie_map.values():
            if movie.poster_path or movie.backdrop_path:
                continue
            ntitle = _normalize_title(movie.title)
            if not ntitle:
                continue
            norm_title_needed.add(ntitle)
            title_year_needed[int(movie.movie_id)] = {
                "norm_title": ntitle,
                "year": int(movie.year) if movie.year else None,
            }

        fallback_candidates = {}

        for chunk in pd.read_csv(tmdb_csv, usecols=usecols, chunksize=chunk_size, low_memory=False):
            chunk["id"] = pd.to_numeric(chunk["id"], errors="coerce")
            chunk = chunk.dropna(subset=["id"])
            chunk["id"] = chunk["id"].astype(int)
            # Pass 1: direct id matches.
            id_chunk = chunk[chunk["id"].isin(tmdb_ids_needed)]
            if not id_chunk.empty:
                for row in id_chunk.to_dict("records"):
                    tmdb_id = int(row["id"])
                    if tmdb_id in tmdb_map:
                        continue
                    tmdb_map[tmdb_id] = {
                        "poster_path": _clean_path(row.get("poster_path")),
                        "backdrop_path": _clean_path(row.get("backdrop_path")),
                        "overview": _clean_text(row.get("overview")),
                        "original_language": _clean_text(row.get("original_language")),
                        "popularity": float(row.get("popularity") or 0.0),
                        "vote_average": float(row.get("vote_average") or 0.0),
                        "vote_count": float(row.get("vote_count") or 0.0),
                        "release_date": _clean_text(row.get("release_date")),
                        "tagline": _clean_text(row.get("tagline")),
                        "tmdb_id": tmdb_id,
                    }

            # Pass 2: title/year fallback candidates.
            if norm_title_needed and ("title" in chunk.columns or "original_title" in chunk.columns):
                rows = chunk.to_dict("records")
                for row in rows:
                    title_variants = []
                    if "title" in row:
                        title_variants.append(row.get("title"))
                    if "original_title" in row:
                        title_variants.append(row.get("original_title"))

                    matched_norm_title = None
                    for t in title_variants:
                        nt = _normalize_title(t)
                        if nt and nt in norm_title_needed:
                            matched_norm_title = nt
                            break

                    if not matched_norm_title:
                        continue

                    poster_path = _clean_path(row.get("poster_path"))
                    backdrop_path = _clean_path(row.get("backdrop_path"))
                    if not (poster_path or backdrop_path):
                        continue

                    popularity = float(row.get("popularity") or 0.0)
                    vote_average = float(row.get("vote_average") or 0.0)
                    vote_count = float(row.get("vote_count") or 0.0)
                    release_date = _clean_text(row.get("release_date"))
                    release_year = _parse_year_from_release_date(release_date)

                    rank_score = (vote_count * 0.01) + popularity + vote_average
                    current = fallback_candidates.get(matched_norm_title)
                    if (current is None) or (rank_score > current["rank_score"]):
                        fallback_candidates[matched_norm_title] = {
                            "poster_path": poster_path,
                            "backdrop_path": backdrop_path,
                            "overview": _clean_text(row.get("overview")),
                            "original_language": _clean_text(row.get("original_language")),
                            "popularity": popularity,
                            "vote_average": vote_average,
                            "vote_count": vote_count,
                            "release_date": release_date,
                            "release_year": release_year,
                            "tagline": _clean_text(row.get("tagline")),
                            "tmdb_id": int(row["id"]),
                            "rank_score": rank_score,
                        }

        self.stdout.write(f"  TMDB id matches collected: {len(tmdb_map):,}")
        self.stdout.write(f"  TMDB fallback title matches collected: {len(fallback_candidates):,}")

        changed = []
        image_filled = 0
        for movie in movie_map.values():
            if not movie.tmdb_id:
                # Fallback: no tmdb_id, try title/year candidate map.
                meta = None
                fallback_key = _normalize_title(movie.title)
                candidate = fallback_candidates.get(fallback_key)
                if candidate:
                    movie_year = int(movie.year) if movie.year else None
                    candidate_year = candidate.get("release_year")
                    if movie_year and candidate_year and abs(movie_year - candidate_year) > 1:
                        candidate = None
                if candidate:
                    meta = candidate
                    if not movie.tmdb_id and candidate.get("tmdb_id"):
                        movie.tmdb_id = int(candidate["tmdb_id"])
                if not meta:
                    continue
            else:
                meta = tmdb_map.get(int(movie.tmdb_id))
                if not meta:
                    # Fallback even when tmdb_id exists but direct id match not found.
                    fallback_key = _normalize_title(movie.title)
                    candidate = fallback_candidates.get(fallback_key)
                    if candidate:
                        movie_year = int(movie.year) if movie.year else None
                        candidate_year = candidate.get("release_year")
                        if movie_year and candidate_year and abs(movie_year - candidate_year) <= 1:
                            meta = candidate
                if not meta:
                    continue

            before_has_image = bool(movie.poster_path or movie.backdrop_path)
            dirty = False

            if meta["poster_path"] and movie.poster_path != meta["poster_path"]:
                movie.poster_path = meta["poster_path"]
                dirty = True
            if meta["backdrop_path"] and movie.backdrop_path != meta["backdrop_path"]:
                movie.backdrop_path = meta["backdrop_path"]
                dirty = True

            if meta["overview"] and not movie.overview:
                movie.overview = meta["overview"]
                dirty = True
            if meta["original_language"] and (not movie.original_language or movie.original_language == "en"):
                movie.original_language = meta["original_language"]
                dirty = True
            if meta["popularity"] and float(movie.popularity or 0.0) == 0.0:
                movie.popularity = meta["popularity"]
                dirty = True
            if meta["vote_average"] and float(movie.vote_average or 0.0) == 0.0:
                movie.vote_average = meta["vote_average"]
                dirty = True
            if meta["release_date"] and not movie.release_date:
                movie.release_date = meta["release_date"]
                dirty = True
            if meta["tagline"] and not movie.tagline:
                movie.tagline = meta["tagline"]
                dirty = True

            after_has_image = bool(movie.poster_path or movie.backdrop_path)
            if (not before_has_image) and after_has_image:
                image_filled += 1

            if dirty:
                changed.append(movie)

        self.stdout.write(f"  Movies changed: {len(changed):,}")
        self.stdout.write(f"  Missing-image movies fixed: {image_filled:,}")

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry run complete (no DB writes)."))
            return

        if linked_count:
            Movie.objects.bulk_update(list(movie_map.values()), ["tmdb_id"], batch_size=batch_size)

        if changed:
            Movie.objects.bulk_update(
                changed,
                [
                    "tmdb_id",
                    "poster_path",
                    "backdrop_path",
                    "overview",
                    "original_language",
                    "popularity",
                    "vote_average",
                    "release_date",
                    "tagline",
                ],
                batch_size=batch_size,
            )

        cache.clear()
        self.stdout.write(self.style.SUCCESS("✅ Movie image sync completed."))