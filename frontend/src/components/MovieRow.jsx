import { memo, useMemo, useRef } from 'react';
import MovieCard from './MovieCard';

const MAX_ROW_MOVIES = 18;

function MovieRow({
  title,
  subtitle,
  movies,
  onMovieClick,
  loading,
  watchlist,
  onToggleWatchlist,
  watched,
  onToggleWatched,
  userId,
  prioritizeImages = false,
}) {
  const sliderRef = useRef(null);
  const visibleMovies = useMemo(
    () => (Array.isArray(movies) ? movies.slice(0, MAX_ROW_MOVIES) : []),
    [movies],
  );
  const eagerThreshold = prioritizeImages ? 6 : 1;
  const highPriorityThreshold = prioritizeImages ? 2 : 1;

  const scroll = (direction) => {
    if (!sliderRef.current) return;

    const slider = sliderRef.current;
    const scrollAmount = slider.clientWidth * 0.8;
    const maxScrollLeft = Math.max(slider.scrollWidth - slider.clientWidth, 0);
    const edgeTolerance = 6;

    if (maxScrollLeft <= 0) return;

    if (direction === 'right') {
      const nearRightEdge = slider.scrollLeft >= (maxScrollLeft - edgeTolerance);
      if (nearRightEdge) {
        slider.scrollTo({
          left: 0,
          behavior: 'smooth',
        });
        return;
      }

      slider.scrollBy({
        left: scrollAmount,
        behavior: 'smooth',
      });
      return;
    }

    const nearLeftEdge = slider.scrollLeft <= edgeTolerance;
    if (nearLeftEdge) {
      slider.scrollTo({
        left: maxScrollLeft,
        behavior: 'smooth',
      });
      return;
    }

    slider.scrollBy({
      left: -scrollAmount,
      behavior: 'smooth',
    });
  };

  if (loading) {
    return (
      <div className="movie-row">
        <div className="movie-row__header">
          <h2 className="movie-row__title">{title}</h2>
        </div>
        <div className="movie-row__slider">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="skeleton skeleton--card" />
          ))}
        </div>
      </div>
    );
  }

  if (visibleMovies.length === 0) return null;

  return (
    <div className="movie-row">
      <div className="movie-row__header">
        <h2 className="movie-row__title">{title}</h2>
        {subtitle && <span className="movie-row__subtitle">{subtitle}</span>}
      </div>

      <div className="movie-row__slider-wrapper">
        <button
          className="movie-row__arrow movie-row__arrow--left"
          onClick={() => scroll('left')}
          aria-label="Scroll left"
        >
          ‹
        </button>

        <div className="movie-row__slider" ref={sliderRef}>
          {visibleMovies.map((item, index) => {
            const movie = item.movie || item;
            if (!movie || typeof movie !== 'object' || movie.movie_id == null) {
              return null;
            }
            return (
              <MovieCard
                key={movie.movie_id}
                movie={movie}
                onClick={onMovieClick}
                watchlist={watchlist}
                onToggleWatchlist={onToggleWatchlist}
                watched={watched}
                onToggleWatched={onToggleWatched}
                userId={userId}
                imageLoading={index < eagerThreshold ? 'eager' : 'lazy'}
                imageFetchPriority={index < highPriorityThreshold ? 'high' : 'auto'}
              />
            );
          })}
        </div>

        <button
          className="movie-row__arrow movie-row__arrow--right"
          onClick={() => scroll('right')}
          aria-label="Scroll right"
        >
          ›
        </button>
      </div>
    </div>
  );
}

export default memo(MovieRow);
