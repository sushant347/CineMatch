import MovieRow from './MovieRow';
import MovieCard from './MovieCard';


function initialsFromName(name) {
  if (!name) return 'CM';
  return name
    .split(' ')
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0].toUpperCase())
    .join('');
}


export default function ProfilePage({
  profile,
  loading,
  watchlistMovies,
  onBack,
  onMovieClick,
  watchlist,
  onToggleWatchlist,
  watched,
  onToggleWatched,
  userId,
}) {
  if (loading) {
    return (
      <section className="profile-page profile-page--loading">
        <div className="profile-page__header skeleton" style={{ height: '220px', borderRadius: '16px' }} />
        <div className="profile-page__section skeleton" style={{ height: '180px', borderRadius: '16px' }} />
      </section>
    );
  }

  if (!profile) {
    return (
      <section className="profile-page">
        <button className="profile-page__back-btn" onClick={onBack} id="profile-back-btn-empty">
          ← Back to Home
        </button>
        <div className="profile-page__empty">
          Select a user profile to view taste insights.
        </div>
      </section>
    );
  }

  const initials = initialsFromName(profile.display_name);
  const genreBars = profile.top_genre_breakdown || [];
  const recentlyRated = profile.recently_rated || [];
  const becauseYouWatched = profile.because_you_watched || [];
  const ratingScale = Number(profile.rating_scale || 10);
  const avgRating = Number(profile.avg_rating || 0).toFixed(2);

  return (
    <section className="profile-page" id="profile-page">
      <div className="profile-page__header">
        <button className="profile-page__back-btn" onClick={onBack} id="profile-back-btn">
          ← Back to Home
        </button>

        <div className="profile-page__identity">
          <div className="profile-page__avatar">{initials}</div>
          <div>
            <h1 className="profile-page__name">{profile.display_name}</h1>
            <p className="profile-page__summary">{profile.taste_summary}</p>
          </div>
        </div>

        <div className="profile-page__stats">
          <div className="profile-page__stat-card">
            <span>Total Ratings</span>
            <strong>{profile.total_ratings}</strong>
          </div>
          <div className="profile-page__stat-card">
            <span>Average Rating</span>
            <strong>{avgRating} / {ratingScale}</strong>
          </div>
          <div className="profile-page__stat-card">
            <span>Top Languages</span>
            <strong>{(profile.top_languages || []).join(' · ') || '—'}</strong>
          </div>
        </div>
      </div>

      <div className="profile-page__section">
        <h2 className="profile-page__section-title">Genre DNA</h2>
        {genreBars.length > 0 ? (
          <div className="profile-page__bars">
            {genreBars.slice(0, 5).map((item) => (
              <div className="profile-page__bar-row" key={item.genre}>
                <div className="profile-page__bar-label">{item.genre}</div>
                <div className="profile-page__bar-track">
                  <div
                    className="profile-page__bar-fill"
                    style={{ width: `${Math.max(6, Number(item.percentage || 0))}%` }}
                  />
                </div>
                <div className="profile-page__bar-value">{item.percentage}%</div>
              </div>
            ))}
          </div>
        ) : (
          <p className="profile-page__muted">Rate more movies to unlock your genre chart.</p>
        )}
      </div>

      {recentlyRated.length > 0 && (
        <div className="profile-page__section">
          <h2 className="profile-page__section-title">Recently Rated</h2>
          <div className="profile-page__recent-grid">
            {recentlyRated.map((entry) => {
              const movie = entry?.movie;
              if (!movie || typeof movie !== 'object' || movie.movie_id == null) {
                return null;
              }
              return (
                <div className="profile-page__recent-item" key={`recent-${movie.movie_id}`}>
                  <MovieCard
                    movie={movie}
                    onClick={onMovieClick}
                    watchlist={watchlist}
                    onToggleWatchlist={onToggleWatchlist}
                    watched={watched}
                    onToggleWatched={onToggleWatched}
                    userId={userId}
                  />
                  <div className="profile-page__recent-rating">★ {Number(entry.rating).toFixed(1)} / {ratingScale}</div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {becauseYouWatched.map((group, index) => (
        <MovieRow
          key={`profile-explained-${index}`}
          title={`Because you watched ${group.source_title}`}
          movies={group.recommendations}
          onMovieClick={onMovieClick}
          watchlist={watchlist}
          onToggleWatchlist={onToggleWatchlist}
          watched={watched}
          onToggleWatched={onToggleWatched}
          userId={userId}
        />
      ))}

      <div className="profile-page__section">
        <h2 className="profile-page__section-title">My Watchlist</h2>
        {watchlistMovies.length > 0 ? (
          <div className="profile-page__watchlist-grid">
            {watchlistMovies.slice(0, 12).map((movie) => {
              if (!movie || typeof movie !== 'object' || movie.movie_id == null) {
                return null;
              }
              return (
                <MovieCard
                  key={`profile-watchlist-${movie.movie_id}`}
                  movie={movie}
                  onClick={onMovieClick}
                  watchlist={watchlist}
                  onToggleWatchlist={onToggleWatchlist}
                  watched={watched}
                  onToggleWatched={onToggleWatched}
                  userId={userId}
                />
              );
            })}
          </div>
        ) : (
          <p className="profile-page__muted">No saved titles yet. Tap bookmark icons to build your list.</p>
        )}
      </div>
    </section>
  );
}
