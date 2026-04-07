from django.urls import path
from . import views

urlpatterns = [
    # Recommendations
    path('recommend/user/<int:user_id>/', views.recommend_for_user, name='recommend-user'),
    path('recommend/movie/<int:movie_id>/', views.recommend_similar, name='recommend-similar'),
    path('explain/<int:user_id>/', views.explained_recommendations, name='explain'),

    # Browse
    path('trending/', views.trending, name='trending'),
    path('hindi/', views.hindi_movies, name='hindi'),
    path('hero/', views.hero_movies, name='hero'),
    path('top-rated/', views.top_rated, name='top-rated'),
    path('genre/<str:genre>/', views.genre_movies, name='genre'),
    path('language/<str:language>/', views.language_movies, name='language'),
    path('search/', views.search_movies, name='search'),

    # Movie detail
    path('movies/<int:movie_id>/', views.movie_detail, name='movie-detail'),

    # Cold start
    path('coldstart/', views.cold_start, name='coldstart'),

    # Home (aggregated data)
    path('home/', views.home_data, name='home'),

    # Users (for demo)
    path('users/', views.user_list, name='users'),
    path('auth/register/', views.register_user, name='register-user'),
    path('auth/login/', views.login_user, name='login-user'),
    path('auth/logout/', views.logout_user, name='logout-user'),
    path('auth/me/', views.auth_me, name='auth-me'),
    path('auth/csrf/', views.auth_csrf, name='auth-csrf'),
    path('auth/preferences/', views.auth_preferences, name='auth-preferences'),

    # Ratings
    path('rate/', views.rate_movie, name='rate-movie'),
    path('ratings/<int:user_id>/', views.get_user_ratings, name='user-ratings'),

    # Watchlist
    path('watchlist/toggle/', views.toggle_watchlist, name='toggle-watchlist'),
    path('watchlist/<int:user_id>/', views.get_watchlist, name='get-watchlist'),

    # Watched
    path('watched/toggle/', views.toggle_watched, name='toggle-watched'),
    path('watched/<int:user_id>/', views.get_watched_movies, name='get-watched'),

    # Reviews
    path('reviews/<int:movie_id>/', views.movie_reviews, name='movie-reviews'),
    path('reviews/write/', views.write_review, name='write-review'),

    # Profile
    path('profile/<int:user_id>/', views.get_user_profile, name='user-profile'),
]
