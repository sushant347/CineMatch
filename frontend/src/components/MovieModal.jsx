import { useState, useEffect, useRef, useMemo } from 'react';
import {
  getMovieDetail,
  getSimilarMovies,
  rateMovie,
  getMovieReviews,
  writeMovieReview,
} from '../api/client';
import MovieCard from './MovieCard';

function StarRating({
  value = 0,
  maxRating = 10,
  step = 1,
  interactive = false,
  onRate,
  disabled = false,
}) {
  const [hoverValue, setHoverValue] = useState(null);
  const safeMax = Math.max(1, Math.round(Number(maxRating) || 10));
  const parsedStep = Number(step);
  const safeStep = parsedStep > 0 ? parsedStep : 1;
  const buttonCount = Math.max(1, Math.round(safeMax / safeStep));
  const valuePrecision = safeStep < 1 ? 1 : 0;
  const displayValue = Math.max(0, Math.min(safeMax, hoverValue ?? value ?? 0));
  const fillPercent = Math.max(0, Math.min(100, (displayValue / safeMax) * 100));

  return (
    <div className={`star-rating ${interactive ? 'interactive' : ''} ${disabled ? 'disabled' : ''}`}>
      <div className="star-rating__display" onMouseLeave={() => setHoverValue(null)}>
        <span className="star-rating__base">{'☆'.repeat(safeMax)}</span>
        <span className="star-rating__fill" style={{ width: `${fillPercent}%` }}>
          {'★'.repeat(safeMax)}
        </span>
        {interactive && (
          <div className="star-rating__buttons">
            {Array.from({ length: buttonCount }).map((_, index) => {
              const nextValue = Number(((index + 1) * safeStep).toFixed(1));
              return (
                <button
                  key={`star-btn-${nextValue}`}
                  type="button"
                  className="star-rating__button"
                  disabled={disabled}
                  onMouseEnter={() => setHoverValue(nextValue)}
                  onFocus={() => setHoverValue(nextValue)}
                  onClick={() => onRate(nextValue)}
                  aria-label={`Rate ${nextValue} out of ${safeMax}`}
                />
              );
            })}
          </div>
        )}
      </div>
      <span className="star-rating__value">{displayValue.toFixed(valuePrecision)} / {safeMax}</span>
    </div>
  );
}


export default function MovieModal({
  movie,
  onClose,
  onMovieClick,
  userId,
  userRatings,
  watchlist,
  onToggleWatchlist,
  watched,
  onToggleWatched,
  onRatingSaved,
  onPreferenceChanged,
}) {
  const [detail, setDetail] = useState(null);
  const [similar, setSimilar] = useState([]);
  const [userRating, setUserRating] = useState(0);
  const [ratingLoading, setRatingLoading] = useState(false);
  const [ratingSaving, setRatingSaving] = useState(false);
  const [ratingToast, setRatingToast] = useState('');
  const [reviews, setReviews] = useState([]);
  const [reviewsLoading, setReviewsLoading] = useState(false);
  const [reviewsError, setReviewsError] = useState('');
  const [reviewText, setReviewText] = useState('');
  const [reviewSaving, setReviewSaving] = useState(false);
  const [reviewError, setReviewError] = useState('');
  const [backdropImageIndex, setBackdropImageIndex] = useState(0);
  const [backdropFailed, setBackdropFailed] = useState(false);
  const detailCacheRef = useRef(new Map());
  const similarCacheRef = useRef(new Map());

  useEffect(() => {
    if (!movie?.movie_id) return;

    let active = true;
    const movieId = Number(movie.movie_id);

    // Immediately show from cache for zero-delay UX
    const cachedDetail = detailCacheRef.current.get(movieId);
    setDetail(cachedDetail || movie);

    const cachedSimilar = similarCacheRef.current.get(movieId);
    if (cachedSimilar) setSimilar(cachedSimilar);
    else setSimilar([]);

    // Fetch full detail in background
    getMovieDetail(movieId)
      .then((payload) => {
        if (!active) return;
        const nextDetail = payload || movie;
        detailCacheRef.current.set(movieId, nextDetail);
        setDetail(nextDetail);
      })
      .catch(() => {
        if (!active) return;
        setDetail(cachedDetail || movie);
      });

    // Defer similar movies fetch by 80ms so modal renders first
    const similarTimer = setTimeout(() => {
      if (!active) return;
      getSimilarMovies(movieId, 12)
        .then((payload) => {
          if (!active) return;
          const nextSimilar = Array.isArray(payload) ? payload : [];
          similarCacheRef.current.set(movieId, nextSimilar);
          setSimilar(nextSimilar);
        })
        .catch(() => {
          if (!active) return;
          setSimilar(cachedSimilar || []);
        });
    }, 80);

    return () => {
      active = false;
      clearTimeout(similarTimer);
    };
  }, [movie]);

  useEffect(() => {
    if (!movie || !userId) {
      setUserRating(0);
      setRatingLoading(false);
      return;
    }

    const movieId = Number(movie.movie_id);
    const cachedRating = Number(userRatings?.[movieId] ?? 0);
    const normalizedRating = Number.isNaN(cachedRating) ? 0 : cachedRating;
    setUserRating(Math.max(0, Math.min(10, normalizedRating)));
    setRatingLoading(false);
  }, [movie, userId, userRatings]);

  useEffect(() => {
    if (!movie?.movie_id) {
      setReviews([]);
      return;
    }

    const movieId = Number(movie.movie_id);
    let active = true;

    setReviewsLoading(true);
    setReviewsError('');
    setReviewError('');

    getMovieReviews(movieId, userId)
      .then((payload) => {
        if (!active) return;
        const next = Array.isArray(payload) ? payload : [];
        setReviews(next);
      })
      .catch((err) => {
        if (!active) return;
        console.error('Failed to load reviews:', err);
        setReviews([]);
        setReviewsError(err?.message || 'Unable to load reviews right now');
      })
      .finally(() => {
        if (active) setReviewsLoading(false);
      });

    return () => {
      active = false;
    };
  }, [movie, userId]);


  useEffect(() => {
    setReviewText('');
    setReviewError('');
  }, [movie?.movie_id]);

  useEffect(() => {
    if (!ratingToast) return undefined;
    const timeoutId = setTimeout(() => setRatingToast(''), 2000);
    return () => clearTimeout(timeoutId);
  }, [ratingToast]);

  useEffect(() => {
    const handleEsc = (e) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handleEsc);
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', handleEsc);
      document.body.style.overflow = '';
    };
  }, [onClose]);

  const d = detail || movie || {};
  const currentMovieId = Number(movie?.movie_id || d.movie_id || 0);
  const backdropCandidates = useMemo(() => {
    const candidates = [
      { url: d.hero_image_url, fit: 'cover' },
      { url: d.backdrop_url, fit: 'cover' },
      { url: d.poster_url, fit: 'contain' },
      { url: movie?.hero_image_url, fit: 'cover' },
      { url: movie?.backdrop_url, fit: 'cover' },
      { url: movie?.poster_url, fit: 'contain' },
    ].filter((item) => Boolean(item.url));

    const unique = [];
    const seen = new Set();
    for (const item of candidates) {
      if (seen.has(item.url)) continue;
      seen.add(item.url);
      unique.push(item);
    }

    return unique;
  }, [
    d.hero_image_url,
    d.backdrop_url,
    d.poster_url,
    movie?.hero_image_url,
    movie?.backdrop_url,
    movie?.poster_url,
  ]);

  const activeBackdrop = backdropCandidates[backdropImageIndex] || { url: '', fit: 'cover' };
  const backdropUrl = activeBackdrop.url;
  const backdropFitClass = activeBackdrop.fit === 'contain'
    ? 'modal__backdrop--contain'
    : 'modal__backdrop--cover';
  const communityRating = Math.max(0, Math.min(10, Number(d.vote_average || 0)));
  const isInWatchlist = Boolean(watchlist?.has(currentMovieId));
  const isWatched = Boolean(watched?.has(currentMovieId));
  const canLeaveFeedback = Boolean(userId && isWatched);

  useEffect(() => {
    setBackdropImageIndex(0);
    setBackdropFailed(false);
  }, [movie?.movie_id]);

  if (!movie) return null;

  const formatReviewDate = (dateValue) => {
    if (!dateValue) return '';
    const parsedDate = new Date(dateValue);
    if (Number.isNaN(parsedDate.getTime())) return '';
    return parsedDate.toLocaleDateString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  };

  const handleRateMovie = async (ratingValue) => {
    if (!userId || !movie?.movie_id || !isWatched) {
      setRatingToast('Mark this movie as watched before rating.');
      return;
    }

    const normalizedRating = Math.max(0.5, Math.min(10, Math.round(Number(ratingValue) * 2) / 2));

    setRatingSaving(true);
    try {
      await rateMovie(userId, movie.movie_id, normalizedRating);
      setUserRating(normalizedRating);
      if (onRatingSaved) {
        onRatingSaved(movie.movie_id, normalizedRating);
      }
      setRatingToast(`Rated ${normalizedRating.toFixed(1)} / 10`);
      if (onPreferenceChanged) {
        onPreferenceChanged();
      }
    } catch (err) {
      console.error('Failed to save rating:', err);
    } finally {
      setRatingSaving(false);
    }
  };

  const handleWatchlistToggle = async () => {
    if (!userId || !onToggleWatchlist) return;
    try {
      await onToggleWatchlist(movie);
    } catch (err) {
      console.error('Watchlist update failed:', err);
    }
  };

  const handleWatchedToggle = async () => {
    if (!userId || !onToggleWatched) return;
    try {
      await onToggleWatched(movie);
    } catch (err) {
      console.error('Watched update failed:', err);
    }
  };

  const handleReviewSubmit = async (event) => {
    event.preventDefault();
    if (!userId || !movie?.movie_id) return;

    if (!isWatched) {
      setReviewError('Mark this movie as watched before writing a review');
      return;
    }

    const trimmed = reviewText.trim();
    if (trimmed.length < 3) {
      setReviewError('Review must be at least 3 characters');
      return;
    }

    setReviewSaving(true);
    setReviewError('');
    try {
      await writeMovieReview(userId, movie.movie_id, trimmed);
      setReviewText('');
      const refreshed = await getMovieReviews(movie.movie_id, userId);
      setReviews(Array.isArray(refreshed) ? refreshed : []);
      setReviewsError('');
    } catch (err) {
      setReviewError(err?.message || 'Unable to save review');
    } finally {
      setReviewSaving(false);
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose} id="movie-modal-overlay">
      <div className="modal" onClick={(e) => e.stopPropagation()} id="movie-modal">
        <button className="modal__close" onClick={onClose} id="modal-close-btn">✕</button>

        <div className="modal__backdrop-wrapper">
          {backdropUrl && !backdropFailed ? (
            <img
              className={`modal__backdrop ${backdropFitClass}`}
              src={backdropUrl}
              alt={d.title}
              loading="eager"
              decoding="async"
              fetchPriority="high"
              referrerPolicy="no-referrer"
              onLoad={() => setBackdropFailed(false)}
              onError={() => {
                if (backdropImageIndex < backdropCandidates.length - 1) {
                  setBackdropImageIndex((prev) => prev + 1);
                  setBackdropFailed(false);
                  return;
                }
                setBackdropFailed(true);
              }}
            />
          ) : (
            <div style={{
              width: '100%', height: '100%',
              background: 'linear-gradient(135deg, #1a1a1a, #2a2a2a)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: '4rem'
            }}>
              🎬
            </div>
          )}
          <div className="modal__backdrop-gradient" />
          <div className="modal__title-overlay">
            <h2 className="modal__title">{d.title}</h2>
          </div>
        </div>

        <div className="modal__body">
          <div className="modal__user-rating">
            <div className="modal__user-rating-title">
              {userId ? 'Your Rating' : 'Community Rating'}
            </div>
            <StarRating
              value={userId ? userRating : communityRating}
              maxRating={10}
              step={0.5}
              interactive={canLeaveFeedback}
              onRate={handleRateMovie}
              disabled={ratingLoading || ratingSaving}
            />
            <p className="modal__user-rating-hint">
              {!userId
                ? 'Login to save ratings and personalize recommendations'
                : (!isWatched
                  ? 'Mark this movie as watched to rate and review'
                  : (ratingLoading ? 'Loading your rating...' : 'Tap left or right side of a star for half-step rating'))}
            </p>
            {ratingToast && <div className="modal__rating-toast">{ratingToast}</div>}

            <div className="modal__actions">
              <button
                className={`modal__action-btn ${isInWatchlist ? 'active' : ''}`}
                onClick={handleWatchlistToggle}
                disabled={!userId}
              >
                {isInWatchlist ? 'In Watchlist' : 'Add to Watchlist'}
              </button>
              <button
                className={`modal__action-btn modal__action-btn--watched ${isWatched ? 'active' : ''}`}
                onClick={handleWatchedToggle}
                disabled={!userId}
              >
                {isWatched ? 'Watched' : 'Mark Watched'}
              </button>
            </div>
          </div>

          <div className="modal__meta-row">
            {d.vote_average > 0 && (
              <span className="modal__rating-badge">⭐ {d.vote_average.toFixed(1)}</span>
            )}
            {d.year && <span className="modal__year">{d.year}</span>}
            {d.original_language && (
              <span className="modal__lang-badge">{d.original_language.toUpperCase()}</span>
            )}
            {d.genres && (
              <span className="modal__year">
                {String(d.genres).split('|').join(' · ')}
              </span>
            )}
          </div>

          {d.tagline && <p className="modal__tagline">"{d.tagline}"</p>}

          {d.overview && <p className="modal__overview">{d.overview}</p>}

          <section className="modal__reviews">
            <h3 className="modal__section-title">Reviews</h3>

            {canLeaveFeedback ? (
              <form className="modal__review-form" onSubmit={handleReviewSubmit}>
                <textarea
                  value={reviewText}
                  onChange={(event) => setReviewText(event.target.value)}
                  placeholder="Write your review..."
                  maxLength={2000}
                />
                {reviewError && <p className="modal__review-error">{reviewError}</p>}
                <button type="submit" disabled={reviewSaving}>
                  {reviewSaving ? 'Saving...' : 'Post Review'}
                </button>
              </form>
            ) : userId ? (
              <p className="modal__review-login-hint">Mark this movie as watched to write a review.</p>
            ) : (
              <p className="modal__review-login-hint">Login to write a review for this movie.</p>
            )}

            {reviewsLoading ? (
              <p className="modal__reviews-empty">Loading reviews...</p>
            ) : reviewsError ? (
              <p className="modal__review-error">{reviewsError}</p>
            ) : reviews.length === 0 ? (
              <p className="modal__reviews-empty">No reviews yet. Be the first to write one.</p>
            ) : (
              <div className="modal__reviews-list">
                {reviews.map((review, index) => (
                  <article
                    className={`modal__review-item ${review.is_mine ? 'mine' : ''}`}
                    key={`${review.user_id}-${index}`}
                  >
                    <header>
                      <strong>{review.user_name || 'Unknown viewer'}</strong>
                      <span>{formatReviewDate(review.updated_at || review.created_at)}</span>
                    </header>
                    <p>{review.review_text}</p>
                  </article>
                ))}
              </div>
            )}
          </section>

          {d.keywords && (
            <dl className="modal__details-grid">
              <dt>Keywords</dt>
              <dd>{String(d.keywords).split(',').slice(0, 6).join(', ')}</dd>
              {d.release_date && (
                <>
                  <dt>Release</dt>
                  <dd>{d.release_date}</dd>
                </>
              )}
            </dl>
          )}

          {similar.length > 0 && (
            <>
              <h3 className="modal__section-title">More Like This</h3>
              <div className="modal__similar-grid">
                {similar.map((item) => {
                  const m = item.movie || item;
                  return (
                    <MovieCard
                      key={m.movie_id}
                      movie={m}
                      onClick={(mov) => {
                        onClose();
                        setTimeout(() => onMovieClick(mov), 200);
                      }}
                      watchlist={watchlist}
                      onToggleWatchlist={onToggleWatchlist}
                      watched={watched}
                      onToggleWatched={onToggleWatched}
                      userId={userId}
                    />
                  );
                })}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
