import re

from rest_framework import serializers
from .models import Movie, UserProfile, Review


USERNAME_PATTERN = re.compile(r'^[A-Za-z0-9_.-]{3,60}$')


class MovieSerializer(serializers.ModelSerializer):
    poster_url = serializers.ReadOnlyField()
    backdrop_url = serializers.ReadOnlyField()
    card_image_url = serializers.ReadOnlyField()
    hero_image_url = serializers.ReadOnlyField()

    class Meta:
        model = Movie
        fields = [
            'movie_id', 'tmdb_id', 'title', 'year', 'genres', 'overview',
            'original_language', 'popularity', 'vote_average',
            'poster_path', 'backdrop_path', 'poster_url', 'backdrop_url',
            'card_image_url', 'hero_image_url',
            'keywords', 'release_date', 'tagline',
        ]


class MovieCardSerializer(serializers.ModelSerializer):
    """Lightweight serializer for movie cards (less data)."""
    poster_url = serializers.ReadOnlyField()
    backdrop_url = serializers.ReadOnlyField()
    card_image_url = serializers.ReadOnlyField()
    hero_image_url = serializers.ReadOnlyField()

    class Meta:
        model = Movie
        fields = [
            'movie_id', 'title', 'year', 'genres', 'vote_average',
            'poster_url', 'backdrop_url', 'card_image_url', 'hero_image_url', 'original_language',
        ]


class RecommendationSerializer(serializers.Serializer):
    """Recommendation with score and explanation."""
    movie = MovieCardSerializer()
    score = serializers.FloatField()
    reason = serializers.CharField(default="")


class ColdStartSerializer(serializers.Serializer):
    """Input for cold start recommendations."""
    genres = serializers.ListField(child=serializers.CharField(), required=False, default=[])
    languages = serializers.ListField(child=serializers.CharField(), required=False, default=["en"])


class RatingSerializer(serializers.Serializer):
    """Input payload for creating or updating a user rating."""
    movie_id = serializers.IntegerField(min_value=1)
    user_id = serializers.IntegerField(min_value=1, required=False)
    rating = serializers.FloatField(min_value=0.5, max_value=10.0)

    def validate_rating(self, value):
        rounded = round(float(value) * 2.0) / 2.0
        if abs(float(value) - rounded) > 1e-6:
            raise serializers.ValidationError("Rating must be in 0.5 increments.")
        return rounded


class WatchlistToggleSerializer(serializers.Serializer):
    """Input payload for adding/removing a movie in watchlist."""
    movie_id = serializers.IntegerField(min_value=1)
    user_id = serializers.IntegerField(min_value=1, required=False)


class WatchedToggleSerializer(serializers.Serializer):
    """Input payload for toggling watched status."""
    movie_id = serializers.IntegerField(min_value=1)
    user_id = serializers.IntegerField(min_value=1, required=False)


class LoginSerializer(serializers.Serializer):
    """Input payload for account login."""
    username = serializers.CharField(min_length=3, max_length=60, trim_whitespace=True)
    password = serializers.CharField(min_length=8, max_length=128, trim_whitespace=False)

    def validate_username(self, value):
        username = value.strip()
        if not USERNAME_PATTERN.fullmatch(username):
            raise serializers.ValidationError(
                'Username may only include letters, numbers, dot, underscore, and hyphen.'
            )
        return username


class RegisterSerializer(serializers.Serializer):
    """Input payload for account signup."""
    username = serializers.CharField(min_length=3, max_length=60, trim_whitespace=True)
    password = serializers.CharField(min_length=8, max_length=128, trim_whitespace=False)
    confirm_password = serializers.CharField(min_length=8, max_length=128, trim_whitespace=False)

    def validate_username(self, value):
        username = value.strip()
        if not USERNAME_PATTERN.fullmatch(username):
            raise serializers.ValidationError(
                'Username may only include letters, numbers, dot, underscore, and hyphen.'
            )
        return username

    def validate(self, attrs):
        if attrs.get('password') != attrs.get('confirm_password'):
            raise serializers.ValidationError({'confirm_password': 'Passwords do not match.'})
        return attrs


class ReviewWriteSerializer(serializers.Serializer):
    """Input payload for creating/updating a review."""
    movie_id = serializers.IntegerField(min_value=1)
    user_id = serializers.IntegerField(min_value=1, required=False)
    review_text = serializers.CharField(min_length=3, max_length=2000)


class ReviewSerializer(serializers.ModelSerializer):
    """Serialized review payload used in movie modal."""
    movie_id = serializers.IntegerField(read_only=True)
    user_name = serializers.CharField(read_only=True)
    is_mine = serializers.BooleanField(read_only=True)

    class Meta:
        model = Review
        fields = ['movie_id', 'user_id', 'user_name', 'review_text', 'is_mine', 'created_at', 'updated_at']


class UserProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserProfile
        fields = '__all__'
