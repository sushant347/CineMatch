"""
API Views for the Recommendation Engine
"""

import re
import time
from collections import Counter

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Avg, Count, Max, Q
from django.contrib.auth import authenticate, get_user_model, login as auth_login, logout as auth_logout
from django.contrib.auth.password_validation import validate_password
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.middleware.csrf import get_token

from .models import Movie, Rating, Watchlist, WatchedMovie, Review, UserAccount, UserProfile
from .serializers import (
    MovieSerializer, MovieCardSerializer,
    ColdStartSerializer, RatingSerializer, WatchlistToggleSerializer,
    WatchedToggleSerializer, LoginSerializer, RegisterSerializer,
    ReviewWriteSerializer, ReviewSerializer
)
from .engine import engine

AUTH_THROTTLE_MAX_ATTEMPTS = 7
AUTH_THROTTLE_WINDOW_SECONDS = 15 * 60
AUTH_THROTTLE_BLOCK_SECONDS = 10 * 60
AUTH_FAILURE_SLEEP_SECONDS = 0.05
USERNAME_SAFE_PATTERN = re.compile(r'^[A-Za-z0-9_.-]{3,60}$')
CUSTOM_USER_CACHE_TTL_SECONDS = 300
HOME_CACHE_TTL_SECONDS = 90
PROFILE_CACHE_TTL_SECONDS = 120
REVIEWS_CACHE_TTL_SECONDS = 45
MOVIE_CARD_ONLY_FIELDS = (
    "movie_id",
    "title",
    "year",
    "genres",
    "vote_average",
    "popularity",
    "poster_path",
    "backdrop_path",
    "original_language",
)
HERO_CACHE_TTL_SECONDS = 300
HERO_PINNED_MOVIE_QUERIES = (
    ("Dune: Part Two", ("dune part two", "dune part 2", "dune 2")),
    ("Oppenheimer", ("oppenheimer",)),
    ("The Dark Knight", ("the dark knight", "dark knight the", "dark knight")),
    ("Avatar", ("avatar",)),
    ("Kantara: A Legend - Chapter 1", ("kantara a legend chapter 1", "kantara chapter 1", "kantara a legend")),
    ("Dhurandhar The Revenge", ("dhurandhar the revenge", "the revenge dhurandhar", "dhurandar the revenge", "the revenge dhurandar")),
    ("F1", ("f1",)),
    ("Spider-Man: No Way Home", ("spider man no way home", "spiderman no way home", "no way home")),
)


def _user_display_name(user_id: int) -> str:
    """Fallback display name for unknown user ids."""
    return f"User {int(user_id)}"


def _resolved_user_name(user_id: int) -> str:
    """Resolve a real username when present, else fallback to numeric user label."""
    user_id = int(user_id)
    cache_key = f"user:display-name:{user_id}"
    try:
        cached_name = cache.get(cache_key)
        if cached_name:
            return cached_name
    except Exception:
        cached_name = None

    username = UserAccount.objects.filter(user_id=user_id).values_list("username", flat=True).first()
    if not username:
        UserModel = get_user_model()
        username = UserModel.objects.filter(id=user_id).values_list("username", flat=True).first()

    if username:
        try:
            cache.set(cache_key, username, timeout=CUSTOM_USER_CACHE_TTL_SECONDS)
        except Exception:
            pass
        return username

    fallback_name = _user_display_name(user_id)
    try:
        cache.set(cache_key, fallback_name, timeout=CUSTOM_USER_CACHE_TTL_SECONDS)
    except Exception:
        pass
    return fallback_name


def _build_user_name_map(user_ids):
    normalized_ids = {int(user_id) for user_id in user_ids if user_id is not None}
    if not normalized_ids:
        return {}

    account_rows = UserAccount.objects.filter(user_id__in=normalized_ids).values_list("user_id", "username")
    name_map = {int(user_id): username for user_id, username in account_rows}

    unresolved_user_ids = [user_id for user_id in normalized_ids if user_id not in name_map]
    if unresolved_user_ids:
        UserModel = get_user_model()
        auth_rows = UserModel.objects.filter(id__in=unresolved_user_ids).values_list("id", "username")
        for auth_id, username in auth_rows:
            if username:
                name_map[int(auth_id)] = username

    for user_id in normalized_ids:
        if user_id not in name_map:
            name_map[user_id] = _user_display_name(user_id)

    return name_map


def _is_generic_review_user_name(user_name: str) -> bool:
    normalized = str(user_name or "").strip().lower()
    if not normalized:
        return True
    if normalized.startswith("user "):
        return True
    if normalized.startswith("reviewer "):
        return True
    return re.fullmatch(r"user\d+", normalized) is not None


def _normalize_title_for_hero_match(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _canonical_review_title(value: str) -> str:
    """Normalize a movie title for review grouping across catalog variants."""
    normalized = _normalize_title_for_hero_match(value)
    normalized = re.sub(r"\b(19|20)\d{2}\b", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _score_hero_title_match(normalized_movie_title: str, movie: Movie, normalized_queries):
    best_score = 0
    for query_index, normalized_query in enumerate(normalized_queries):
        if not normalized_query:
            continue

        query_weight = 9000 - (query_index * 300)
        if normalized_movie_title == normalized_query:
            best_score = max(best_score, query_weight + 5000)
            continue

        if normalized_movie_title.startswith(f"{normalized_query} "):
            best_score = max(best_score, query_weight + 3200)
            continue

        if normalized_query in normalized_movie_title:
            best_score = max(best_score, query_weight + 2200)

    if best_score <= 0:
        return 0

    return best_score + int(float(movie.vote_average or 0.0) * 100) + int(float(movie.popularity or 0.0))


def _select_pinned_hero_movies(limit: int):
    candidates = list(_movie_card_queryset().order_by("-vote_average", "-popularity", "-movie_id"))
    normalized_candidates = [
        (movie, _normalize_title_for_hero_match(movie.title))
        for movie in candidates
    ]

    selected_movies = []
    selected_ids = set()

    for _, query_aliases in HERO_PINNED_MOVIE_QUERIES:
        normalized_queries = [
            _normalize_title_for_hero_match(alias)
            for alias in query_aliases
            if str(alias or "").strip()
        ]
        best_movie = None
        best_score = 0

        for movie, normalized_title in normalized_candidates:
            movie_id = int(movie.movie_id)
            if movie_id in selected_ids:
                continue

            score = _score_hero_title_match(normalized_title, movie, normalized_queries)
            if score > best_score:
                best_score = score
                best_movie = movie

        if best_movie is not None and best_score > 0:
            selected_movies.append(best_movie)
            selected_ids.add(int(best_movie.movie_id))

        if len(selected_movies) >= limit:
            break

    return selected_movies[:limit]


def _is_custom_user(user_id: int) -> bool:
    normalized_user_id = int(user_id)
    cache_key = f"user:is-custom:{normalized_user_id}"
    try:
        cached_value = cache.get(cache_key)
    except Exception:
        cached_value = None

    if cached_value is not None:
        return bool(cached_value)

    exists = UserAccount.objects.filter(user_id=normalized_user_id).exists()
    try:
        cache.set(cache_key, bool(exists), timeout=CUSTOM_USER_CACHE_TTL_SECONDS)
    except Exception:
        pass
    return bool(exists)


def _client_ip(request) -> str:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.META.get("REMOTE_ADDR") or "unknown").strip()


def _auth_attempt_keys(request, username: str):
    ip = _client_ip(request)
    safe_username = (username or "").strip().lower() or "anonymous"
    return [
        f"auth:attempt:ip:{ip}",
        f"auth:attempt:user:{safe_username}",
    ]


def _auth_is_blocked(cache_key: str):
    data = cache.get(cache_key) or {}
    blocked_until = float(data.get("blocked_until") or 0)
    now = time.time()
    if blocked_until > now:
        return True, int(blocked_until - now)
    return False, 0


def _auth_rate_limit_response(request, username: str):
    max_seconds = 0
    for cache_key in _auth_attempt_keys(request, username):
        blocked, wait_seconds = _auth_is_blocked(cache_key)
        if blocked:
            max_seconds = max(max_seconds, wait_seconds)

    if max_seconds > 0:
        return Response(
            {
                "error": "Too many authentication attempts. Please try again shortly.",
                "retry_after": max_seconds,
            },
            status=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    return None


def _auth_record_failure(request, username: str):
    ttl_seconds = max(AUTH_THROTTLE_WINDOW_SECONDS, AUTH_THROTTLE_BLOCK_SECONDS)
    now = time.time()

    for cache_key in _auth_attempt_keys(request, username):
        payload = cache.get(cache_key) or {"count": 0, "blocked_until": 0}
        if float(payload.get("blocked_until") or 0) > now:
            continue

        count = int(payload.get("count") or 0) + 1
        blocked_until = float(payload.get("blocked_until") or 0)
        if count >= AUTH_THROTTLE_MAX_ATTEMPTS:
            blocked_until = now + AUTH_THROTTLE_BLOCK_SECONDS

        cache.set(
            cache_key,
            {
                "count": count,
                "blocked_until": blocked_until,
            },
            timeout=ttl_seconds,
        )


def _auth_clear_failures(request, username: str):
    for cache_key in _auth_attempt_keys(request, username):
        cache.delete(cache_key)


def _get_authenticated_account(request):
    if not request.user or not request.user.is_authenticated:
        return None
    username = (request.user.username or "").strip()
    if not username:
        return None

    account = UserAccount.objects.filter(username__iexact=username).first()
    if account:
        return account

    account = UserAccount.objects.create(
        username=username,
        password_hash="",
        user_id=_next_custom_user_id(),
    )
    UserProfile.objects.get_or_create(user_id=account.user_id)
    try:
        cache.delete(f"user:is-custom:{int(account.user_id)}")
        cache.delete(f"user:display-name:{int(account.user_id)}")
    except Exception:
        pass
    return account


def _forbidden_if_private_user(request, target_user_id: int):
    """Prevent access to custom account data unless owner is authenticated."""
    target_user_id = int(target_user_id)
    if not _is_custom_user(target_user_id):
        return None

    account = _get_authenticated_account(request)
    if account and int(account.user_id) == target_user_id:
        return None

    return Response(
        {"error": "Forbidden"},
        status=status.HTTP_403_FORBIDDEN,
    )


def _require_authenticated_account(request):
    if not request.user or not request.user.is_authenticated:
        return None, Response(
            {"error": "Authentication required"},
            status=status.HTTP_401_UNAUTHORIZED,
        )

    account = _get_authenticated_account(request)
    if not account:
        return None, Response(
            {"error": "Authentication required"},
            status=status.HTTP_401_UNAUTHORIZED,
        )
    return account, None


def _custom_user_liked_threshold(user_id: int) -> float:
    if _is_custom_user(user_id):
        return 8.0
    return 4.0


def _build_account_payload(account: UserAccount, request):
    stats = Rating.objects.filter(user_id=account.user_id).aggregate(
        count=Count("id"),
        avg_rating=Avg("rating"),
    )

    profile, _ = UserProfile.objects.get_or_create(user_id=account.user_id)
    genres = [
        item.strip()
        for item in str(profile.favorite_genres or "").split("|")
        if item.strip()
    ]
    languages = [
        item.strip()
        for item in str(profile.preferred_languages or "").split(",")
        if item.strip()
    ]
    if not languages:
        languages = ["en"]

    return {
        "success": True,
        "user_id": int(account.user_id),
        "user_name": account.username,
        "rating_count": int(stats.get("count") or 0),
        "avg_rating": round(float(stats.get("avg_rating") or 0.0), 2),
        "is_custom": True,
        "csrfToken": get_token(request),
        "preferences": {
            "genres": genres,
            "languages": languages,
        },
    }


def _next_custom_user_id() -> int:
    """Allocate a new numeric user_id for custom accounts."""
    ratings_max_user = Rating.objects.aggregate(max_user=Max("user_id"))
    account_max = UserAccount.objects.aggregate(max_user=Max("user_id"))
    profile_max = UserProfile.objects.aggregate(max_user=Max("user_id"))

    max_user_id = max(
        int(ratings_max_user.get("max_user") or 0),
        int(account_max.get("max_user") or 0),
        int(profile_max.get("max_user") or 0),
    )
    return max_user_id + 1


def _parse_limit(raw_value, default=20, minimum=1, maximum=200):
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(parsed, maximum))


def _movies_with_images_queryset():
    return Movie.objects.filter(
        Q(poster_path__gt="") | Q(backdrop_path__gt="")
    )


def _movie_card_queryset():
    return _movies_with_images_queryset().only(*MOVIE_CARD_ONLY_FIELDS)


def _review_cache_version(movie_id: int) -> int:
    cache_key = f"api:reviews:version:{int(movie_id)}"
    try:
        version = int(cache.get(cache_key) or 1)
    except Exception:
        version = 1
    return max(version, 1)


def _bump_review_cache_version(movie_id: int):
    cache_key = f"api:reviews:version:{int(movie_id)}"
    try:
        current = int(cache.get(cache_key) or 1)
    except Exception:
        current = 1

    try:
        cache.set(cache_key, current + 1, timeout=24 * 60 * 60)
    except Exception:
        pass


def _cached_response_payload(cache_key, timeout_seconds, builder):
    try:
        cached_payload = cache.get(cache_key)
        if cached_payload is not None:
            return cached_payload
    except Exception:
        cached_payload = None

    payload = builder()

    try:
        cache.set(cache_key, payload, timeout=timeout_seconds)
    except Exception:
        pass

    return payload


def _build_movie_payload_map(movie_ids):
    unique_ids = {int(movie_id) for movie_id in movie_ids if movie_id is not None}
    if not unique_ids:
        return {}

    movies = _movie_card_queryset().filter(movie_id__in=unique_ids)
    serialized = MovieCardSerializer(movies, many=True).data
    return {int(item["movie_id"]): item for item in serialized}


def _enrich_recommendations(rec_list, movie_payload_map=None):
    """Convert engine results to serialized movie data with scores."""
    if not rec_list:
        return []

    if movie_payload_map is None:
        movie_payload_map = _build_movie_payload_map(
            [item.get("movie_id") for item in rec_list]
        )

    enriched = []
    for rec in rec_list:
        movie_id = int(rec.get("movie_id"))
        movie_payload = movie_payload_map.get(movie_id)
        if movie_payload:
            enriched.append(
                {
                    "movie": movie_payload,
                    "score": rec.get("score", 0.0),
                    "reason": rec.get("reason", ""),
                }
            )
    return enriched


def _movie_queryset_recommendations(queryset, reason=""):
    """Convert a Movie queryset/list into API recommendation payload shape."""
    movies = list(queryset)
    if not movies:
        return []

    serialized_movies = MovieCardSerializer(movies, many=True).data
    payload = []
    for index, movie in enumerate(movies):
        payload.append(
            {
                "movie": serialized_movies[index],
                "score": float(movie.popularity or 0.0),
                "reason": reason,
            }
        )
    return payload


def _db_trending_payload(n):
    movies = _movie_card_queryset().order_by("-popularity", "-vote_average")[:n]
    return _movie_queryset_recommendations(movies, reason="Trending now")


def _db_hindi_payload(n):
    movies = (
        _movie_card_queryset()
        .filter(original_language__iexact="hi")
        .order_by("-popularity", "-vote_average")[:n]
    )
    return _movie_queryset_recommendations(movies, reason="Popular Hindi")


def _db_similar_payload(movie_id, n):
    """Fast DB-only similar movie fallback used by modal "More Like This"."""
    movie = _movie_card_queryset().filter(movie_id=movie_id).first()
    if not movie:
        return []

    movie_genres = [g.strip() for g in str(movie.genres or "").split("|") if g.strip()]
    language = (movie.original_language or "").strip().lower()

    selected_payload = []
    selected_ids = {int(movie_id)}

    def append_movies(queryset, reason):
        rows = list(queryset)
        if not rows:
            return

        serialized_rows = MovieCardSerializer(rows, many=True).data
        for index, row in enumerate(rows):
            row_id = int(row.movie_id)
            if row_id in selected_ids:
                continue

            selected_ids.add(row_id)
            selected_payload.append(
                {
                    "movie": serialized_rows[index],
                    "score": float(row.popularity or 0.0),
                    "reason": reason,
                }
            )
            if len(selected_payload) >= n:
                break

    genre_q = Q()
    for genre in movie_genres[:3]:
        genre_q |= Q(genres__icontains=genre)

    genre_reason = "Similar movie"
    if movie_genres:
        genre_reason = f"Similar {' · '.join(movie_genres[:2])} film"

    if genre_q:
        if language:
            append_movies(
                _movie_card_queryset()
                .exclude(movie_id__in=selected_ids)
                .filter(genre_q)
                .filter(original_language__iexact=language)
                .order_by("-vote_average", "-popularity")[: n * 3],
                genre_reason,
            )

        if len(selected_payload) < n:
            append_movies(
                _movie_card_queryset()
                .exclude(movie_id__in=selected_ids)
                .filter(genre_q)
                .order_by("-vote_average", "-popularity")[: n * 4],
                genre_reason,
            )

    if len(selected_payload) < n and language:
        append_movies(
            _movie_card_queryset()
            .exclude(movie_id__in=selected_ids)
            .filter(original_language__iexact=language)
            .order_by("-popularity", "-vote_average")[: n * 3],
            f"Popular {language.upper()} pick",
        )

    if len(selected_payload) < n:
        append_movies(
            _movie_card_queryset()
            .exclude(movie_id__in=selected_ids)
            .order_by("-popularity", "-vote_average")[: n * 3],
            "Popular pick",
        )

    return selected_payload[:n]


@api_view(["GET"])
def recommend_for_user(request, user_id):
    """Hybrid recommendations for a specific user."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    n = _parse_limit(request.query_params.get("n"), default=20, maximum=100)
    recs = engine.get_user_recommendations(user_id, n=n)
    return Response(_enrich_recommendations(recs))


@api_view(["GET"])
def recommend_similar(request, movie_id):
    """Fast similar movies feed for modal cards (DB-first, no ML warmup)."""
    n = _parse_limit(request.query_params.get("n"), default=12, maximum=100)
    cache_key = f"api:similar:db:{int(movie_id)}:{n}:v2"
    return Response(_cached_response_payload(cache_key, 300, lambda: _db_similar_payload(movie_id, n)))



@api_view(["GET"])
def trending(request):
    """Trending movies by popularity."""
    n = _parse_limit(request.query_params.get("n"), default=20, maximum=100)
    cache_key = f"api:trending:db:{n}"
    return Response(_cached_response_payload(cache_key, 180, lambda: _db_trending_payload(n)))


@api_view(["GET"])
def hindi_movies(request):
    """Popular Hindi movies."""
    n = _parse_limit(request.query_params.get("n"), default=20, maximum=100)
    cache_key = f"api:hindi:db:{n}"
    return Response(_cached_response_payload(cache_key, 180, lambda: _db_hindi_payload(n)))


@api_view(["GET"])
def search_movies(request):
    """Search movies by title."""
    q = request.query_params.get("q", "").strip()
    if not q:
        return Response([])

    n = _parse_limit(request.query_params.get("n"), default=20, maximum=200)
    cache_key = f"api:search:{q.lower()}:{n}"

    def build_payload():
        base_queryset = _movie_card_queryset()

        startswith_movies = list(
            base_queryset.filter(title__istartswith=q)
            .order_by("-popularity", "-vote_average")[:n]
        )
        startswith_ids = {movie.movie_id for movie in startswith_movies}

        contains_movies = []
        if len(startswith_movies) < n:
            contains_movies = list(
                base_queryset.filter(title__icontains=q)
                .exclude(movie_id__in=startswith_ids)
                .order_by("-popularity", "-vote_average")[: max(n - len(startswith_movies), 0)]
            )

        movies = startswith_movies + contains_movies
        serialized_movies = MovieCardSerializer(movies, many=True).data

        payload = []
        for index, movie in enumerate(movies):
            payload.append(
                {
                    "movie": serialized_movies[index],
                    "score": float(movie.popularity or 0.0),
                    "reason": "",
                }
            )
        return payload

    payload = _cached_response_payload(cache_key, 120, build_payload)
    return Response(payload)


@api_view(["GET"])
def movie_detail(request, movie_id):
    """Get full movie details."""
    try:
        movie = Movie.objects.get(movie_id=movie_id)
        return Response(MovieSerializer(movie).data)
    except Movie.DoesNotExist:
        return Response({"error": "Movie not found"}, status=status.HTTP_404_NOT_FOUND)


@api_view(["POST"])
def cold_start(request):
    """Cold start: recommend based on genre/language preferences."""
    serializer = ColdStartSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    genres = serializer.validated_data.get("genres", [])
    languages = serializer.validated_data.get("languages", ["en"])

    recs = engine.get_cold_start_recommendations(genres=genres, languages=languages, n=20)
    return Response(_enrich_recommendations(recs))


@api_view(["GET"])
def explained_recommendations(request, user_id):
    """Get recommendations with 'Because you watched X' explanations."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    groups = engine.get_explained_recommendations(user_id, n=20)
    explained_movie_ids = []
    for group in groups:
        explained_movie_ids.extend(
            item.get("movie_id") for item in group.get("recommendations", [])
        )
    movie_payload_map = _build_movie_payload_map(explained_movie_ids)

    enriched_groups = []
    for group in groups:
        enriched_recs = _enrich_recommendations(group["recommendations"], movie_payload_map)
        enriched_groups.append({
            "source_title": group["source_title"],
            "source_movie_id": group["source_movie_id"],
            "recommendations": enriched_recs,
        })

    return Response(enriched_groups)


@api_view(["GET"])
def user_list(request):
    """Return only the currently authenticated custom account."""
    account = _get_authenticated_account(request)
    if not account:
        return Response([])

    stats = Rating.objects.filter(user_id=account.user_id).aggregate(
        count=Count("id"),
        avg_rating=Avg("rating"),
    )
    return Response([
        {
            "user_id": int(account.user_id),
            "user_name": account.username,
            "rating_count": int(stats.get("count") or 0),
            "avg_rating": round(float(stats.get("avg_rating") or 0.0), 2),
            "is_custom": True,
        }
    ])


@api_view(["POST"])
def login_user(request):
    """Authenticate a user and start a secure session."""
    serializer = LoginSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    username = serializer.validated_data["username"].strip()
    password = serializer.validated_data["password"]
    if not USERNAME_SAFE_PATTERN.fullmatch(username):
        return Response(
            {"error": "Invalid username or password"},
            status=status.HTTP_401_UNAUTHORIZED,
        )

    blocked_response = _auth_rate_limit_response(request, username)
    if blocked_response:
        return blocked_response

    UserModel = get_user_model()

    user = authenticate(request, username=username, password=password)

    try:
        if user is None or not user.is_active:
            _auth_record_failure(request, username)
            time.sleep(AUTH_FAILURE_SLEEP_SECONDS)
            return Response(
                {"error": "Invalid username or password"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        auth_login(request, user)
        request.session.cycle_key()

        account = UserAccount.objects.filter(username__iexact=user.username).first()
        if not account:
            account = UserAccount.objects.create(
                username=user.username,
                password_hash="",
                user_id=_next_custom_user_id(),
            )
            UserProfile.objects.get_or_create(user_id=account.user_id)
            try:
                cache.delete(f"user:is-custom:{int(account.user_id)}")
                cache.delete(f"user:display-name:{int(account.user_id)}")
            except Exception:
                pass

        _auth_clear_failures(request, username)
        return Response(_build_account_payload(account, request))
    except Exception:
        _auth_record_failure(request, username)
        return Response(
            {"error": "Unable to login right now"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def register_user(request):
    """Create a new account and start an authenticated session."""
    serializer = RegisterSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    username = serializer.validated_data["username"].strip()
    password = serializer.validated_data["password"]

    blocked_response = _auth_rate_limit_response(request, username)
    if blocked_response:
        return blocked_response

    UserModel = get_user_model()
    if UserModel.objects.filter(username__iexact=username).exists() or UserAccount.objects.filter(username__iexact=username).exists():
        _auth_record_failure(request, username)
        return Response(
            {"error": "Username is already taken"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        validate_password(password, user=UserModel(username=username))
    except ValidationError as exc:
        details = [str(message) for message in exc.messages if str(message).strip()]
        if not details:
            details = ["Password does not meet security requirements."]
        return Response(
            {
                "error": details[0],
                "details": details,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        user = UserModel.objects.create_user(username=username, password=password)
        account = UserAccount.objects.create(
            username=user.username,
            password_hash="",
            user_id=_next_custom_user_id(),
        )
        UserProfile.objects.get_or_create(user_id=account.user_id)
        try:
            cache.delete(f"user:is-custom:{int(account.user_id)}")
            cache.delete(f"user:display-name:{int(account.user_id)}")
        except Exception:
            pass

        auth_login(request, user)
        request.session.cycle_key()
        _auth_clear_failures(request, username)
        return Response(_build_account_payload(account, request), status=status.HTTP_201_CREATED)
    except Exception:
        _auth_record_failure(request, username)
        return Response(
            {"error": "Unable to create account right now"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def logout_user(request):
    """Destroy current authenticated session."""
    auth_logout(request)
    return Response({"success": True})


@api_view(["GET"])
def auth_me(request):
    """Return current authenticated account details."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error
    return Response(_build_account_payload(account, request))


@api_view(["GET"])
def auth_csrf(request):
    """Ensure a CSRF token cookie exists and return the current token."""
    return Response({"csrfToken": get_token(request)})


@api_view(["GET", "POST"])
def auth_preferences(request):
    """Read/update authenticated user's language and genre preferences."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error

    def clean_list(values, lower=False):
        cleaned = []
        seen = set()

        for item in values or []:
            value = str(item).strip()
            if not value:
                continue

            normalized = value.lower() if lower else value
            dedupe_key = normalized.lower()
            if dedupe_key in seen:
                continue

            seen.add(dedupe_key)
            cleaned.append(normalized)

        return cleaned

    profile, _ = UserProfile.objects.get_or_create(user_id=account.user_id)

    if request.method == "GET":
        genres_raw = [
            item.strip()
            for item in str(profile.favorite_genres or "").split("|")
            if item.strip()
        ]
        languages_raw = [
            item.strip()
            for item in str(profile.preferred_languages or "").split(",")
            if item.strip()
        ]
        genres = clean_list(genres_raw)
        languages = clean_list(languages_raw, lower=True)
        if not languages:
            languages = ["en"]
        return Response(
            {
                "genres": genres,
                "languages": languages,
            }
        )

    serializer = ColdStartSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    genres = clean_list(serializer.validated_data.get("genres", []))
    languages = clean_list(serializer.validated_data.get("languages", []), lower=True)
    if not languages:
        languages = ["en"]

    profile.favorite_genres = "|".join(genres)
    profile.preferred_languages = ",".join(languages)
    profile.save(update_fields=["favorite_genres", "preferred_languages"])

    return Response(
        {
            "success": True,
            "genres": genres,
            "languages": languages,
        }
    )


@api_view(["GET"])
def home_data(request):
    """Get all data needed for the home page in one call."""
    user_id = request.query_params.get("user_id")

    if not user_id:
        cache_key = "api:home:anon:v2"

        def build_anon_payload():
            return {
                "trending": _db_trending_payload(15),
                "hindi": _db_hindi_payload(15),
            }

        return Response(_cached_response_payload(cache_key, HOME_CACHE_TTL_SECONDS, build_anon_payload))

    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    cache_key = f"api:home:user:{user_id}:v3"

    def build_user_payload():
        trending_recs = engine.get_trending(n=15)
        hindi_recs = engine.get_hindi_movies(n=15)
        recommended_recs = engine.get_user_recommendations(user_id, n=15)
        explained_groups = engine.get_explained_recommendations(user_id, n=15)

        movie_ids = [item.get("movie_id") for item in trending_recs]
        movie_ids.extend(item.get("movie_id") for item in hindi_recs)
        movie_ids.extend(item.get("movie_id") for item in recommended_recs)
        for group in explained_groups:
            movie_ids.extend(item.get("movie_id") for item in group.get("recommendations", []))

        movie_payload_map = _build_movie_payload_map(movie_ids)
        data = {
            "trending": _enrich_recommendations(trending_recs, movie_payload_map),
            "hindi": _enrich_recommendations(hindi_recs, movie_payload_map),
            "recommended": _enrich_recommendations(recommended_recs, movie_payload_map),
            "explained": [],
        }

        for group in explained_groups:
            data["explained"].append(
                {
                    "source_title": group["source_title"],
                    "source_movie_id": group["source_movie_id"],
                    "recommendations": _enrich_recommendations(
                        group.get("recommendations", []),
                        movie_payload_map,
                    ),
                }
            )

        return data

    return Response(_cached_response_payload(cache_key, HOME_CACHE_TTL_SECONDS, build_user_payload))


@api_view(["GET"])
def top_rated(request):
    """Get top rated movies."""
    n = _parse_limit(request.query_params.get("n"), default=20, maximum=100)
    cache_key = f"api:top-rated:{n}"

    def build_payload():
        movies = (
            _movie_card_queryset()
            .filter(vote_average__gte=7.0)
            .order_by("-vote_average", "-popularity")[:n]
        )
        return MovieCardSerializer(movies, many=True).data

    return Response(_cached_response_payload(cache_key, 300, build_payload))


@api_view(["GET"])
def hero_movies(request):
    """Get hero picks pinned to requested movie titles in fixed order."""
    n = _parse_limit(request.query_params.get("n"), default=8, maximum=8)
    cache_key = f"api:hero:pinned:{n}:v5"

    def build_payload():
        pinned_movies = _select_pinned_hero_movies(limit=n)
        return MovieCardSerializer(pinned_movies, many=True).data

    return Response(_cached_response_payload(cache_key, HERO_CACHE_TTL_SECONDS, build_payload))


@api_view(["GET"])
def genre_movies(request, genre):
    """Get movies by genre."""
    n = _parse_limit(request.query_params.get("n"), default=20, maximum=160)
    normalized_genre = str(genre or "").strip().lower()
    cache_key = f"api:genre:{normalized_genre}:{n}"

    def build_payload():
        movies = (
            _movie_card_queryset()
            .filter(genres__icontains=genre)
            .order_by("-popularity")[:n]
        )
        return MovieCardSerializer(movies, many=True).data

    return Response(_cached_response_payload(cache_key, 900, build_payload))


@api_view(["GET"])
def language_movies(request, language):
    """Get movies by original language."""
    n = _parse_limit(request.query_params.get("n"), default=20, maximum=160)
    normalized_language = str(language or "").strip().lower()
    cache_key = f"api:language:{normalized_language}:{n}"

    def build_payload():
        movies = (
            _movie_card_queryset()
            .filter(original_language__iexact=language)
            .order_by("-popularity")[:n]
        )
        return MovieCardSerializer(movies, many=True).data

    return Response(_cached_response_payload(cache_key, 300, build_payload))


@api_view(["POST"])
def rate_movie(request):
    """Create or update a user's rating for a movie."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error

    serializer = RatingSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    payload_user_id = serializer.validated_data.get("user_id")
    user_id = int(account.user_id)
    if payload_user_id and int(payload_user_id) != user_id:
        return Response(
            {"error": "Forbidden"},
            status=status.HTTP_403_FORBIDDEN,
        )

    movie_id = serializer.validated_data["movie_id"]
    rating_input = float(serializer.validated_data["rating"])
    rating_value = round(max(0.5, min(10.0, rating_input)) * 2.0) / 2.0

    try:
        movie = Movie.objects.get(movie_id=movie_id)
    except Movie.DoesNotExist:
        return Response(
            {"error": "Movie not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception:
        return Response(
            {"error": "Unable to load movie"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    if not WatchedMovie.objects.filter(user_id=user_id, movie=movie).exists():
        return Response(
            {"error": "Mark this movie as watched before rating it."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        Rating.objects.update_or_create(
            user_id=user_id,
            movie=movie,
            defaults={
                "rating": rating_value,
                "timestamp": int(time.time()),
            },
        )

        engine.invalidate_user_cache(user_id)
        return Response(
            {
                "success": True,
                "movie_id": movie_id,
                "rating": float(rating_value),
                "vote_average": float(movie.vote_average or 0.0),
            }
        )
    except Exception:
        return Response(
            {"error": "Unable to save rating"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_user_ratings(request, user_id):
    """Return all ratings for a user as movie_id/rating pairs."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    try:
        raw_ratings = list(
            Rating.objects.filter(user_id=user_id)
            .order_by("-timestamp", "-id")
            .values("movie_id", "rating")
        )
        ratings = [
            {
                "movie_id": int(item["movie_id"]),
                "rating": round(float(item["rating"]), 1),
            }
            for item in raw_ratings
        ]
        return Response(ratings)
    except Exception:
        return Response(
            {"error": "Unable to fetch user ratings"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def toggle_watchlist(request):
    """Add/remove a movie in a user's watchlist."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error

    serializer = WatchlistToggleSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    payload_user_id = serializer.validated_data.get("user_id")
    user_id = int(account.user_id)
    if payload_user_id and int(payload_user_id) != user_id:
        return Response(
            {"error": "Forbidden"},
            status=status.HTTP_403_FORBIDDEN,
        )

    movie_id = serializer.validated_data["movie_id"]

    try:
        movie = Movie.objects.get(movie_id=movie_id)
    except Movie.DoesNotExist:
        return Response(
            {"error": "Movie not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception:
        return Response(
            {"error": "Unable to load movie"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    try:
        existing = Watchlist.objects.filter(user_id=user_id, movie=movie).first()
        if existing:
            existing.delete()
            action = "removed"
        else:
            Watchlist.objects.create(user_id=user_id, movie=movie)
            action = "added"

        return Response(
            {
                "success": True,
                "action": action,
                "movie_id": movie_id,
            }
        )
    except Exception:
        return Response(
            {"error": "Unable to update watchlist"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_watchlist(request, user_id):
    """Get a user's watchlist as serialized movie objects."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    try:
        entries = Watchlist.objects.filter(user_id=user_id).select_related("movie").order_by("-added_at")
        movies = [entry.movie for entry in entries]
        return Response(MovieCardSerializer(movies, many=True).data)
    except Exception:
        return Response(
            {"error": "Unable to fetch watchlist"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def toggle_watched(request):
    """Toggle watched status for a movie and user."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error

    serializer = WatchedToggleSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    payload_user_id = serializer.validated_data.get("user_id")
    user_id = int(account.user_id)
    if payload_user_id and int(payload_user_id) != user_id:
        return Response(
            {"error": "Forbidden"},
            status=status.HTTP_403_FORBIDDEN,
        )

    movie_id = serializer.validated_data["movie_id"]

    try:
        movie = Movie.objects.get(movie_id=movie_id)
    except Movie.DoesNotExist:
        return Response(
            {"error": "Movie not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception:
        return Response(
            {"error": "Unable to load movie"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    try:
        watched = WatchedMovie.objects.filter(user_id=user_id, movie=movie).first()
        if watched:
            watched.delete()
            action = "removed"
            is_watched = False
        else:
            WatchedMovie.objects.create(user_id=user_id, movie=movie)
            action = "added"
            is_watched = True

        engine.invalidate_user_cache(user_id)
        return Response(
            {
                "success": True,
                "action": action,
                "is_watched": is_watched,
                "movie_id": movie_id,
            }
        )
    except Exception:
        return Response(
            {"error": "Unable to update watched status"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_watched_movies(request, user_id):
    """Get watched movie IDs for a user."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    try:
        watched_ids = list(
            WatchedMovie.objects.filter(user_id=user_id)
            .order_by("-watched_at")
            .values_list("movie_id", flat=True)
        )
        return Response(
            {
                "user_id": int(user_id),
                "movie_ids": watched_ids,
                "count": len(watched_ids),
            }
        )
    except Exception:
        return Response(
            {"error": "Unable to fetch watched movies"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def movie_reviews(request, movie_id):
    """Get reviews for a movie (most recent first)."""
    account = _get_authenticated_account(request)
    viewer_user_id = int(account.user_id) if account else 0

    limit = _parse_limit(request.query_params.get("n"), default=50, maximum=200)

    try:
        movie_id = int(movie_id)
    except (TypeError, ValueError):
        return Response(
            {"error": "Invalid movie_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    def serialize_reviews(review_rows, is_related_title=False):
        if not review_rows:
            return []

        name_map = _build_user_name_map([row["user_id"] for row in review_rows])
        payload = []

        for row in review_rows:
            resolved_user_name = str(name_map.get(int(row["user_id"]), "")).strip()
            if _is_generic_review_user_name(resolved_user_name):
                resolved_user_name = f"Viewer {int(row['user_id'])}"

            payload.append(
                {
                    "movie_id": int(row["movie_id"]),
                    "user_id": int(row["user_id"]),
                    "user_name": resolved_user_name,
                    "review_text": row["review_text"],
                    "is_mine": False,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "is_fallback": bool(is_related_title),
                    "is_related_title": bool(is_related_title),
                    "source_movie_id": int(row["movie_id"]) if is_related_title else None,
                    "source_movie_title": str(row.get("movie__title") or "") if is_related_title else "",
                }
            )

        return payload

    def primary_review_rows():
        return list(
            Review.objects.filter(movie_id=movie_id)
            .order_by("-updated_at", "-created_at")
            .values("movie_id", "user_id", "review_text", "created_at", "updated_at")[:limit]
        )

    def related_title_review_rows():
        source_movie = Movie.objects.filter(movie_id=movie_id).only(
            "movie_id", "title", "year"
        ).first()
        if not source_movie:
            return []

        canonical_title = _canonical_review_title(source_movie.title)
        if not canonical_title:
            return []

        search_seed = canonical_title.split(" ")[0]
        if not search_seed:
            return []

        source_year = int(source_movie.year) if source_movie.year else None

        candidate_movies = list(
            Movie.objects.exclude(movie_id=movie_id)
            .only("movie_id", "title", "year")
            .filter(title__icontains=search_seed)
            .order_by("-popularity")[:300]
        )

        related_movie_ids = []
        for candidate_movie in candidate_movies:
            if _canonical_review_title(candidate_movie.title) != canonical_title:
                continue

            if source_year and candidate_movie.year:
                if abs(int(candidate_movie.year) - source_year) > 1:
                    continue

            related_movie_ids.append(int(candidate_movie.movie_id))
            if len(related_movie_ids) >= 80:
                break

        if not related_movie_ids:
            return []

        return list(
            Review.objects.filter(movie_id__in=related_movie_ids)
            .order_by("-updated_at", "-created_at")
            .values(
                "movie_id",
                "movie__title",
                "user_id",
                "review_text",
                "created_at",
                "updated_at",
            )[:limit]
        )

    def build_reviews_payload():
        direct_reviews = serialize_reviews(primary_review_rows(), is_related_title=False)
        if direct_reviews:
            return direct_reviews
        return serialize_reviews(related_title_review_rows(), is_related_title=True)

    try:
        review_version = _review_cache_version(movie_id)
        cache_key = f"api:reviews:{int(movie_id)}:{int(limit)}:v{review_version}:v3"
        cached_payload = _cached_response_payload(cache_key, REVIEWS_CACHE_TTL_SECONDS, build_reviews_payload)

        if viewer_user_id <= 0:
            return Response(cached_payload)

        viewer_id = int(viewer_user_id)
        personalized_payload = [
            {
                **row,
                "is_mine": int(row.get("user_id") or 0) == viewer_id,
            }
            for row in cached_payload
        ]
        return Response(personalized_payload)
    except Exception:
        return Response(
            {"error": "Unable to fetch reviews"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def write_review(request):
    """Create or update a user's review for a movie."""
    account, auth_error = _require_authenticated_account(request)
    if auth_error:
        return auth_error

    serializer = ReviewWriteSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    payload_user_id = serializer.validated_data.get("user_id")
    user_id = int(account.user_id)
    if payload_user_id and int(payload_user_id) != user_id:
        return Response(
            {"error": "Forbidden"},
            status=status.HTTP_403_FORBIDDEN,
        )

    movie_id = serializer.validated_data["movie_id"]
    review_text = serializer.validated_data["review_text"].strip()

    try:
        movie = Movie.objects.get(movie_id=movie_id)
    except Movie.DoesNotExist:
        return Response(
            {"error": "Movie not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception:
        return Response(
            {"error": "Unable to load movie"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    if not WatchedMovie.objects.filter(user_id=user_id, movie=movie).exists():
        return Response(
            {"error": "Mark this movie as watched before writing a review."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        review_obj, _ = Review.objects.update_or_create(
            user_id=user_id,
            movie=movie,
            defaults={"review_text": review_text},
        )
        _bump_review_cache_version(movie_id)
        review_obj.user_name = _resolved_user_name(int(user_id))
        review_obj.is_mine = True
        return Response(
            {
                "success": True,
                "review": ReviewSerializer(review_obj).data,
            }
        )
    except Exception:
        return Response(
            {"error": "Unable to save review"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_user_profile(request, user_id):
    """Aggregate profile/taste data for a selected demo user."""
    if user_id <= 0:
        return Response(
            {"error": "Invalid user_id"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    forbidden = _forbidden_if_private_user(request, user_id)
    if forbidden:
        return forbidden

    cache_key = f"api:profile:{int(user_id)}:v2"

    def build_profile_payload():
        rating_scale = 10.0 if _is_custom_user(user_id) else 5.0
        ratings_qs = Rating.objects.filter(user_id=user_id).select_related("movie")
        total_ratings = ratings_qs.count()
        avg_rating = ratings_qs.aggregate(avg=Avg("rating"))["avg"] or 0.0

        liked_ratings = ratings_qs.filter(rating__gte=_custom_user_liked_threshold(user_id))
        genre_counter = Counter()
        for item in liked_ratings:
            for genre in str(item.movie.genres).split("|"):
                cleaned = genre.strip()
                if cleaned:
                    genre_counter[cleaned] += 1

        language_counter = Counter()
        for item in ratings_qs:
            language = (item.movie.original_language or "").strip()
            if language:
                language_counter[language] += 1

        top_genre_items = genre_counter.most_common(5)
        top_genres = [name for name, _ in top_genre_items[:3]]
        top_languages = [name for name, _ in language_counter.most_common(3)]

        if len(top_genres) >= 2:
            taste_summary = (
                f"You love {top_genres[0]} and {top_genres[1]} stories with strong rewatch value."
            )
        elif len(top_genres) == 1:
            taste_summary = f"You have a clear preference for {top_genres[0]} films."
        else:
            taste_summary = "Keep rating movies to unlock a sharper taste profile."

        recently_rated_payload = []
        recent_ratings = ratings_qs.order_by("-timestamp", "-id")[:5]
        for entry in recent_ratings:
            recently_rated_payload.append(
                {
                    "movie": MovieCardSerializer(entry.movie).data,
                    "rating": float(entry.rating),
                }
            )

        explained_groups = []
        raw_explained_groups = engine.get_explained_recommendations(user_id, n=15)
        explained_movie_ids = []
        for group in raw_explained_groups:
            explained_movie_ids.extend(
                item.get("movie_id") for item in group.get("recommendations", [])
            )
        explained_movie_payload_map = _build_movie_payload_map(explained_movie_ids)

        for group in raw_explained_groups:
            explained_groups.append(
                {
                    "source_title": group["source_title"],
                    "source_movie_id": group["source_movie_id"],
                    "recommendations": _enrich_recommendations(
                        group["recommendations"],
                        explained_movie_payload_map,
                    ),
                }
            )

        total_genre_votes = sum(count for _, count in top_genre_items) or 1
        top_genre_breakdown = [
            {
                "genre": genre,
                "count": count,
                "percentage": round((count / total_genre_votes) * 100, 1),
            }
            for genre, count in top_genre_items
        ]

        return {
            "user_id": user_id,
            "display_name": _resolved_user_name(user_id),
            "total_ratings": total_ratings,
            "avg_rating": round(float(avg_rating), 2),
            "rating_scale": rating_scale,
            "top_genres": top_genres,
            "top_languages": top_languages,
            "taste_summary": taste_summary,
            "recently_rated": recently_rated_payload,
            "because_you_watched": explained_groups,
            "top_genre_breakdown": top_genre_breakdown,
        }

    try:
        return Response(
            _cached_response_payload(
                cache_key,
                PROFILE_CACHE_TTL_SECONDS,
                build_profile_payload,
            )
        )
    except Exception:
        return Response(
            {"error": "Unable to build profile"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
