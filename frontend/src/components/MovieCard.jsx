import { memo, useEffect, useMemo, useRef, useState } from 'react';

const IMAGE_STALL_TIMEOUT_MS = 4000;
const CARD_POSTER_WIDTH = 342;
const CARD_POSTER_HEIGHT = 513;

function buildTmdbSrcSet(url) {
  const normalizedUrl = String(url || '').trim();
  if (!normalizedUrl.includes('image.tmdb.org/t/p/')) {
    return {
      src: normalizedUrl,
      srcSet: undefined,
      sizes: undefined,
    };
  }

  const match = normalizedUrl.match(/\/t\/p\/(w\d+|original)\/(.+)$/i);
  if (!match) {
    return {
      src: normalizedUrl,
      srcSet: undefined,
      sizes: undefined,
    };
  }

  const assetPath = match[2];
  const base = normalizedUrl.slice(0, normalizedUrl.indexOf('/t/p/'));
  const srcSet = [
    `${base}/t/p/w185/${assetPath} 185w`,
    `${base}/t/p/w342/${assetPath} 342w`,
    `${base}/t/p/w500/${assetPath} 500w`,
  ].join(', ');

  return {
    src: `${base}/t/p/w342/${assetPath}`,
    srcSet,
    sizes: '(max-width: 640px) 48vw, (max-width: 900px) 32vw, 20vw',
  };
}

function MovieCard({
  movie,
  onClick,
  watchlist,
  onToggleWatchlist,
  watched,
  onToggleWatched,
  userId,
  imageLoading = 'lazy',
  imageFetchPriority = 'auto',
}) {
  const safeMovie = movie && typeof movie === 'object' ? movie : null;
  const movieId = Number(safeMovie?.movie_id);

  const imageCandidates = useMemo(() => {
    if (!safeMovie) return [];
    // Keep portrait-first candidates to avoid card cropping/stretching.
    const urls = [
      safeMovie.card_image_url,
      safeMovie.poster_url,
    ].filter(Boolean);
    return Array.from(new Set(urls));
  }, [safeMovie?.card_image_url, safeMovie?.poster_url, safeMovie?.backdrop_url]);

  const [imageCandidateIndex, setImageCandidateIndex] = useState(0);
  const activeImageUrl = imageCandidates[imageCandidateIndex] || '';
  const activeImage = useMemo(
    () => buildTmdbSrcSet(activeImageUrl),
    [activeImageUrl],
  );

  const isHindi = safeMovie?.original_language === 'hi';
  const isInWatchlist = Boolean(watchlist?.has(movieId));
  const isWatched = Boolean(watched?.has(movieId));

  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipText, setTooltipText] = useState('');
  const [imgFailed, setImgFailed] = useState(false);
  const [imgLoading, setImgLoading] = useState(Boolean(activeImageUrl));

  const tooltipTimer = useRef(null);
  const stallTimerRef = useRef(null);

  const clearStallTimer = () => {
    if (stallTimerRef.current) {
      clearTimeout(stallTimerRef.current);
      stallTimerRef.current = null;
    }
  };

  useEffect(() => {
    return () => {
      if (tooltipTimer.current) {
        clearTimeout(tooltipTimer.current);
      }
      clearStallTimer();
    };
  }, []);

  useEffect(() => {
    setImageCandidateIndex(0);
    setImgFailed(false);
    setImgLoading(Boolean(imageCandidates[0]));
    clearStallTimer();
  }, [movieId, imageCandidates]);

  useEffect(() => {
    if (!activeImageUrl || imgFailed || !imgLoading) {
      clearStallTimer();
      return undefined;
    }

    clearStallTimer();
    stallTimerRef.current = setTimeout(() => {
      setImgLoading(false);
    }, IMAGE_STALL_TIMEOUT_MS);

    return () => {
      clearStallTimer();
    };
  }, [activeImageUrl, imgFailed, imgLoading, imageCandidateIndex]);

  const handleCardClick = () => {
    if (onClick && safeMovie) onClick(safeMovie);
  };

  const showLoginTooltip = (message) => {
    setTooltipText(message);
    setShowTooltip(true);
    if (tooltipTimer.current) clearTimeout(tooltipTimer.current);
    tooltipTimer.current = setTimeout(() => setShowTooltip(false), 1800);
  };

  const handleImageError = () => {
    clearStallTimer();
    if (imageCandidateIndex < imageCandidates.length - 1) {
      setImageCandidateIndex((prev) => prev + 1);
      setImgFailed(false);
      setImgLoading(true);
      return;
    }

    setImgFailed(true);
    setImgLoading(false);
  };

  const handleWatchlistClick = async (event) => {
    event.stopPropagation();

    if (!userId) {
      showLoginTooltip('Login to save this movie');
      return;
    }

    if (onToggleWatchlist && safeMovie) {
      try {
        await onToggleWatchlist(safeMovie);
      } catch (err) {
        console.error('Watchlist update failed:', err);
      }
    }
  };

  const handleWatchedClick = async (event) => {
    event.stopPropagation();

    if (!userId) {
      showLoginTooltip('Login to mark movies as watched');
      return;
    }

    if (onToggleWatched && safeMovie) {
      try {
        await onToggleWatched(safeMovie);
      } catch (err) {
        console.error('Watched update failed:', err);
      }
    }
  };

  if (!safeMovie || Number.isNaN(movieId)) {
    return null;
  }

  return (
    <div
      className="movie-card"
      onClick={handleCardClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && handleCardClick()}
      id={`movie-card-${safeMovie.movie_id}`}
    >
      <button
        className={`movie-card__watched-btn ${isWatched ? 'active' : ''}`}
        onClick={handleWatchedClick}
        aria-label={isWatched ? 'Unmark watched' : 'Mark as watched'}
        id={`watched-btn-${safeMovie.movie_id}`}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M4 12.5l5 5L20 6.5" />
        </svg>
      </button>

      <button
        className={`movie-card__watchlist-btn ${isInWatchlist ? 'active' : ''}`}
        onClick={handleWatchlistClick}
        aria-label={isInWatchlist ? 'Remove from watchlist' : 'Add to watchlist'}
        id={`watchlist-btn-${safeMovie.movie_id}`}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M6 3.5h12a1 1 0 0 1 1 1V21l-7-4-7 4V4.5a1 1 0 0 1 1-1z" />
        </svg>
      </button>
      {showTooltip && (
        <div className="movie-card__watchlist-tooltip">{tooltipText}</div>
      )}

      {isHindi && (
        <span className="movie-card__lang-badge movie-card__lang-badge--hi">
          हिंदी
        </span>
      )}
      {!isHindi && safeMovie.original_language && safeMovie.original_language !== 'en' && (
        <span className="movie-card__lang-badge">
          {safeMovie.original_language}
        </span>
      )}

      {activeImageUrl && !imgFailed ? (
        <div className="movie-card__media">
          {imgLoading && <div className="skeleton movie-card__image-skeleton" />}
          <img
            className={`movie-card__poster ${imgLoading ? 'is-loading' : ''}`}
            src={activeImage.src || activeImageUrl}
            srcSet={activeImage.srcSet}
            sizes={activeImage.sizes}
            alt={safeMovie.title}
            loading={imageLoading}
            decoding="async"
            fetchPriority={imageFetchPriority}
            referrerPolicy="no-referrer"
            width={CARD_POSTER_WIDTH}
            height={CARD_POSTER_HEIGHT}
            draggable={false}
            onLoad={() => {
              clearStallTimer();
              setImgLoading(false);
            }}
            onError={handleImageError}
          />
        </div>
      ) : (
        <div className="movie-card__no-poster">
          <div>
            <div style={{ fontSize: '2rem', marginBottom: '8px' }}>🎬</div>
            <div>{safeMovie.title}</div>
          </div>
        </div>
      )}

      <div className="movie-card__info">
        <div className="movie-card__title">{safeMovie.title}</div>
        <div className="movie-card__meta">
          {safeMovie.vote_average > 0 && (
            <span className="movie-card__rating">⭐ {safeMovie.vote_average.toFixed(1)}</span>
          )}
          {safeMovie.year && <span>{safeMovie.year}</span>}
        </div>
        {safeMovie.genres && (
          <div className="movie-card__genre">
            {String(safeMovie.genres).split('|').slice(0, 2).join(' · ')}
          </div>
        )}
      </div>
    </div>
  );
}

function areCardPropsEqual(prevProps, nextProps) {
  const prevMovie = prevProps.movie;
  const nextMovie = nextProps.movie;
  const prevMovieId = Number(prevMovie?.movie_id);
  const nextMovieId = Number(nextMovie?.movie_id);

  if (prevMovieId !== nextMovieId) return false;

  if (!prevMovie || !nextMovie) {
    return prevMovie === nextMovie;
  }

  if (
    prevMovie.title !== nextMovie.title
    || prevMovie.vote_average !== nextMovie.vote_average
    || prevMovie.year !== nextMovie.year
    || prevMovie.genres !== nextMovie.genres
    || prevMovie.original_language !== nextMovie.original_language
    || prevMovie.card_image_url !== nextMovie.card_image_url
    || prevMovie.backdrop_url !== nextMovie.backdrop_url
    || prevMovie.poster_url !== nextMovie.poster_url
  ) {
    return false;
  }

  if (prevProps.userId !== nextProps.userId) return false;
  if (prevProps.imageLoading !== nextProps.imageLoading) return false;
  if (prevProps.imageFetchPriority !== nextProps.imageFetchPriority) return false;

  const watchlistChanged = Boolean(prevProps.watchlist?.has(nextMovieId)) !== Boolean(nextProps.watchlist?.has(nextMovieId));
  if (watchlistChanged) return false;

  const watchedChanged = Boolean(prevProps.watched?.has(nextMovieId)) !== Boolean(nextProps.watched?.has(nextMovieId));
  if (watchedChanged) return false;

  return (
    prevProps.onClick === nextProps.onClick
    && prevProps.onToggleWatchlist === nextProps.onToggleWatchlist
    && prevProps.onToggleWatched === nextProps.onToggleWatched
  );
}

export default memo(MovieCard, areCardPropsEqual);
