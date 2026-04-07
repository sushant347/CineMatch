from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from urllib.parse import quote
from functools import lru_cache


class Movie(models.Model):
    """Movie metadata from merged MovieLens + TMDB dataset."""
    movie_id = models.IntegerField(primary_key=True, help_text="MovieLens movieId")
    tmdb_id = models.IntegerField(null=True, blank=True)
    title = models.CharField(max_length=500, db_index=True)
    year = models.IntegerField(null=True, blank=True)
    genres = models.CharField(max_length=500, default="")
    overview = models.TextField(default="")
    original_language = models.CharField(max_length=10, default="en", db_index=True)
    popularity = models.FloatField(default=0, db_index=True)
    vote_average = models.FloatField(default=0, db_index=True)
    poster_path = models.CharField(max_length=300, default="")
    backdrop_path = models.CharField(max_length=300, default="")
    keywords = models.TextField(default="")
    release_date = models.CharField(max_length=20, default="")
    tagline = models.CharField(max_length=500, default="")

    class Meta:
        ordering = ['-popularity']
        indexes = [
            models.Index(
                fields=['original_language', 'popularity'],
                name='rec_movie_lang_pop_idx',
            ),
            models.Index(
                fields=['vote_average', 'popularity'],
                name='rec_movie_vote_pop_idx',
            ),
        ]

    def __str__(self):
        return f"{self.title} ({self.year})"

    @staticmethod
    def _tmdb_url(path, size):
        path = str(path or "").strip()
        if not path:
            return ""
        if path.lower().startswith(("http://", "https://")):
            return path

        # Some catalogs store IMDb CDN paths as /images/... (without scheme/host).
        # These are not TMDB paths, so route them to IMDb directly.
        if path.startswith("images/"):
            path = f"/{path}"
        if path.startswith("/images/"):
            return f"https://m.media-amazon.com{path}"

        if not path.startswith("/"):
            path = f"/{path}"
        return f"https://image.tmdb.org/t/p/{size}{path}"

    @staticmethod
    @lru_cache(maxsize=8)
    def _placeholder_data_url(width, height):
        svg = (
            f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' "
            f"viewBox='0 0 {width} {height}'>"
            "<defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>"
            "<stop offset='0%' stop-color='#1f1f1f'/><stop offset='100%' stop-color='#121212'/>"
            "</linearGradient></defs>"
            f"<rect width='{width}' height='{height}' fill='url(#g)'/>"
            f"<text x='{width // 2}' y='{height // 2}' fill='#e5e5e5' font-size='22' "
            "text-anchor='middle' dominant-baseline='middle' "
            "font-family='Arial, sans-serif'>No Image</text>"
            "</svg>"
        )
        return f"data:image/svg+xml;charset=UTF-8,{quote(svg)}"

    @property
    def card_image_url(self):
        if self.poster_path:
            return self._tmdb_url(self.poster_path, "w185")
        if self.backdrop_path:
            return self._tmdb_url(self.backdrop_path, "w300")
        return self._placeholder_data_url(342, 513)

    @property
    def hero_image_url(self):
        if self.backdrop_path:
            return self._tmdb_url(self.backdrop_path, "w500")
        if self.poster_path:
            return self._tmdb_url(self.poster_path, "w342")
        return self._placeholder_data_url(1280, 720)

    @property
    def poster_url(self):
        if self.poster_path:
            return self._tmdb_url(self.poster_path, "w342")
        if self.backdrop_path:
            return self._tmdb_url(self.backdrop_path, "w500")
        return self._placeholder_data_url(500, 750)

    @property
    def backdrop_url(self):
        if self.backdrop_path:
            return self._tmdb_url(self.backdrop_path, "w780")
        if self.poster_path:
            return self._tmdb_url(self.poster_path, "w500")
        return self._placeholder_data_url(1280, 720)


class Rating(models.Model):
    """User rating for a movie."""
    user_id = models.IntegerField(db_index=True)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name='ratings')
    rating = models.FloatField(
        validators=[MinValueValidator(0.5), MaxValueValidator(10.0)]
    )
    timestamp = models.IntegerField(null=True, blank=True)

    class Meta:
        unique_together = ('user_id', 'movie')
        indexes = [
            models.Index(fields=['user_id', 'rating']),
            models.Index(fields=['user_id', 'timestamp'], name='rec_rating_user_time_idx'),
        ]

    def __str__(self):
        return f"User {self.user_id} → {self.movie.title}: {self.rating}"


class Watchlist(models.Model):
    """Movies a user has saved to their watchlist."""
    user_id = models.IntegerField(db_index=True)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name='watchlisted_by')
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user_id', 'movie')
        indexes = [
            models.Index(fields=['user_id', 'added_at']),
        ]

    def __str__(self):
        return f"User {self.user_id} watchlist → {self.movie.title}"


class WatchedMovie(models.Model):
    """Movies a user has already watched."""
    user_id = models.IntegerField(db_index=True)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name='watched_by')
    watched_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user_id', 'movie')
        indexes = [
            models.Index(fields=['user_id', 'watched_at']),
        ]

    def __str__(self):
        return f"User {self.user_id} watched → {self.movie.title}"


class Review(models.Model):
    """Short text review a user writes for a movie."""
    user_id = models.IntegerField(db_index=True)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name='reviews')
    review_text = models.TextField(max_length=2000)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('user_id', 'movie')
        indexes = [
            models.Index(fields=['movie', 'created_at']),
            models.Index(fields=['user_id', 'updated_at']),
            models.Index(fields=['movie', 'updated_at', 'created_at'], name='rec_review_movie_recent_idx'),
        ]

    def __str__(self):
        return f"Review by user {self.user_id} on {self.movie.title}"


class UserAccount(models.Model):
    """Lightweight login account tied to recommendation user_id."""
    username = models.CharField(max_length=60, unique=True)
    password_hash = models.CharField(max_length=255)
    user_id = models.IntegerField(unique=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.username} ({self.user_id})"


class UserProfile(models.Model):
    """User preferences for cold start and personalization."""
    user_id = models.IntegerField(primary_key=True)
    preferred_languages = models.CharField(max_length=200, default="en")
    favorite_genres = models.CharField(max_length=500, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Profile for User {self.user_id}"
