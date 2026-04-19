import { memo, useEffect, useMemo, useRef, useState, useCallback } from 'react';
import { createPortal } from 'react-dom';

const IMAGE_STALL_TIMEOUT_MS = 4000;
const HOVER_PREVIEW_DELAY_MS = 1000;
const INLINE_HOVER_PREVIEW_DELAY_MS = 160;
const CARD_POSTER_WIDTH = 780;
const CARD_POSTER_HEIGHT = 439;
const POPUP_WIDTH = 300;

function buildTmdbSrcSet(url) {
  const normalizedUrl = String(url || '').trim();
  if (!normalizedUrl.includes('image.tmdb.org/t/p/')) {
    return { src: normalizedUrl, srcSet: undefined, sizes: undefined };
  }
  const match = normalizedUrl.match(/\/t\/p\/(w\d+|original)\/(.+)$/i);
  if (!match) {
    return { src: normalizedUrl, srcSet: undefined, sizes: undefined };
  }
  const assetPath = match[2];
  const base = normalizedUrl.slice(0, normalizedUrl.indexOf('/t/p/'));
  const srcSet = [
    `${base}/t/p/w185/${assetPath} 185w`,
    `${base}/t/p/w300/${assetPath} 300w`,
    `${base}/t/p/w500/${assetPath} 500w`,
    `${base}/t/p/w780/${assetPath} 780w`,
  ].join(', ');
  return {
    src: `${base}/t/p/w300/${assetPath}`,
    srcSet,
    sizes: '(max-width: 640px) 52vw, (max-width: 900px) 36vw, 22vw',
  };
}

/* ── Portal Popup ── */
function PopupPortal({ movie, anchorRect, isVisible, onToggleWatchlist, onToggleWatched,
  watchlist, watched, userId, onOpen, popupImageUrl, onPopupEnter, onPopupLeave }) {
  const [pos, setPos] = useState({ top: 0, left: 0 });

  useEffect(() => {
    if (!anchorRect) return;
    const vw = window.innerWidth;

    // The popup image must EXACTLY cover the card — same top, same center
    const cardCenterX = anchorRect.left + anchorRect.width / 2;
    const popupWidth = Math.max(anchorRect.width * 1.5, POPUP_WIDTH); // 1.5x the card width
    let left = cardCenterX - popupWidth / 2;
    // Clamp horizontally within viewport
    left = Math.max(8, Math.min(left, vw - popupWidth - 8));

    // Top aligned with card top — image overlaps the card, info drops below
    const top = anchorRect.top;

    setPos({ top, left, popupWidth });
  }, [anchorRect]);

  if (!movie) return null;

  const movieId = Number(movie.movie_id);
  const isInWatchlist = Boolean(watchlist?.has(movieId));
  const isWatched = Boolean(watched?.has(movieId));
  const popupImage = buildTmdbSrcSet(popupImageUrl);

  const handleWatchlist = (e) => {
    e.stopPropagation();
    if (!userId) return;
    onToggleWatchlist?.(movie);
  };

  const handleWatched = (e) => {
    e.stopPropagation();
    if (!userId) return;
    onToggleWatched?.(movie);
  };

  const handleOpenFromPopupImage = (event) => {
    event.stopPropagation();
    onOpen?.();
  };

  const el = (
    <div
      className={`movie-card-popup-portal${isVisible ? ' is-visible' : ''}`}
      data-movie-card-popup="true"
      style={{
        top: pos.top,
        left: pos.left,
        width: pos.popupWidth || POPUP_WIDTH,
        // image block height must match the real card height so they perfectly overlap
        '--card-height': anchorRect ? `${anchorRect.height}px` : '0px',
      }}
      onClick={(e) => e.stopPropagation()}
      onMouseEnter={onPopupEnter}
      onMouseLeave={onPopupLeave}
      onFocusCapture={onPopupEnter}
      onBlurCapture={(event) => {
        if (event.currentTarget.contains(event.relatedTarget)) return;
        onPopupLeave?.();
      }}
    >
      {/* Image — 16:9, top-anchored, always fills the frame */}
      {popupImageUrl && (
        <div
          className="movie-card-popup-portal__media"
          role="button"
          tabIndex={0}
          aria-label={`Open ${movie.title} details`}
          onClick={handleOpenFromPopupImage}
          onKeyDown={(event) => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault();
              handleOpenFromPopupImage(event);
            }
          }}
        >
          <img
            key={popupImageUrl}
            className="movie-card-popup-portal__poster"
            src={popupImage.src || popupImageUrl}
            srcSet={popupImage.srcSet}
            sizes="300px"
            alt={movie.title}
            loading="eager"
            decoding="async"
            referrerPolicy="no-referrer"
            draggable={false}
          />
        </div>
      )}

      {/* Info body */}
      <div className="movie-card-popup-portal__body">
        {/* Action buttons row */}
        <div className="movie-card__popup-actions-row">
          <div className="movie-card__popup-actions-left">
            <button
              className={`movie-card__popup-circle-btn${isInWatchlist ? ' active' : ''}`}
              onClick={handleWatchlist}
              aria-label={isInWatchlist ? 'Remove from watchlist' : 'Add to watchlist'}
            >
              {isInWatchlist ? '✓' : '+'}
            </button>
            <button
              className="movie-card__popup-circle-btn"
              onClick={handleWatched}
              aria-label={isWatched ? 'Mark unwatched' : 'Mark watched'}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13">
                <path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3zM7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3" />
              </svg>
            </button>
          </div>
          <button
            className="movie-card__popup-circle-btn"
            onClick={(e) => { e.stopPropagation(); onOpen?.(); }}
            aria-label={`Open ${movie.title}`}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
              <polyline points="6 9 12 15 18 9" />
            </svg>
          </button>
        </div>

        {/* Title */}
        <div className="movie-card__title-sleek">{movie.title}</div>

        {/* Metadata row */}
        <div className="movie-card__popup-metadata-row">
          <span className="movie-card__match-score">
            {movie.vote_average ? `${Math.round(movie.vote_average * 10)}% match` : ''}
          </span>
          <span className="movie-card__age-rating">U/A 16+</span>
          {(movie.runtime || movie.duration) && (
            <span className="movie-card__duration">{movie.runtime || movie.duration} min</span>
          )}
          <span className="movie-card__hd-badge">HD</span>
        </div>

        {/* Genres */}
        {movie.genres && (
          <div className="movie-card__popup-genres-row">
            {String(movie.genres).split('|').join(' • ')}
          </div>
        )}
      </div>
    </div>
  );

  return createPortal(el, document.body);
}

/* ── Movie Card ── */
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
  popupVariant = 'detached',
}) {
  const safeMovie = movie && typeof movie === 'object' ? movie : null;
  const movieId = Number(safeMovie?.movie_id);

  const imageCandidates = useMemo(() => {
    if (!safeMovie) return [];
    const wideUrls = [
      safeMovie.hero_image_url,
      safeMovie.backdrop_url,
      safeMovie.card_image_url,
    ].filter(Boolean);
    const urls = wideUrls.length > 0 ? wideUrls : [safeMovie.poster_url].filter(Boolean);
    return Array.from(new Set(urls));
  }, [safeMovie?.hero_image_url, safeMovie?.card_image_url, safeMovie?.poster_url, safeMovie?.backdrop_url]);

  const [imageCandidateIndex, setImageCandidateIndex] = useState(0);
  const activeImageUrl = imageCandidates[imageCandidateIndex] || '';
  const activeImage = useMemo(() => buildTmdbSrcSet(activeImageUrl), [activeImageUrl]);

  // Popup image: prefer wide backdrop for a good 16:9 crop
  const popupImageUrl = useMemo(
    () => safeMovie?.backdrop_url
      || safeMovie?.card_image_url
      || safeMovie?.hero_image_url
      || safeMovie?.poster_url
      || activeImageUrl,
    [safeMovie?.poster_url, safeMovie?.card_image_url, safeMovie?.backdrop_url, safeMovie?.hero_image_url, activeImageUrl],
  );

  const isHindi = safeMovie?.original_language === 'hi';
  const isInWatchlist = Boolean(watchlist?.has(movieId));
  const isWatched = Boolean(watched?.has(movieId));

  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipText, setTooltipText] = useState('');
  const [imgFailed, setImgFailed] = useState(false);
  const [imgLoading, setImgLoading] = useState(Boolean(activeImageUrl));
  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [anchorRect, setAnchorRect] = useState(null);

  const cardRef = useRef(null);
  const mediaRef = useRef(null);
  const tooltipTimer = useRef(null);
  const stallTimerRef = useRef(null);
  const previewTimerRef = useRef(null);
  const popupHoverRef = useRef(false);

  const clearStallTimer = useCallback(() => {
    if (stallTimerRef.current) { clearTimeout(stallTimerRef.current); stallTimerRef.current = null; }
  }, []);

  const clearPreviewTimer = useCallback(() => {
    if (previewTimerRef.current) { clearTimeout(previewTimerRef.current); previewTimerRef.current = null; }
  }, []);

  const updateAnchorRect = useCallback(() => {
    if (!cardRef.current) return;
    setAnchorRect(cardRef.current.getBoundingClientRect());
  }, []);

  useEffect(() => {
    return () => {
      if (tooltipTimer.current) clearTimeout(tooltipTimer.current);
      clearStallTimer();
      clearPreviewTimer();
    };
  }, [clearPreviewTimer, clearStallTimer]);

  useEffect(() => {
    setImageCandidateIndex(0);
    setImgFailed(false);
    setImgLoading(Boolean(imageCandidates[0]));
    setIsPreviewOpen(false);
    popupHoverRef.current = false;
    clearPreviewTimer();
    clearStallTimer();
  }, [movieId, imageCandidates, clearPreviewTimer, clearStallTimer]);

  useEffect(() => {
    if (!activeImageUrl || imgFailed || !imgLoading) { clearStallTimer(); return undefined; }
    clearStallTimer();
    stallTimerRef.current = setTimeout(() => setImgLoading(false), IMAGE_STALL_TIMEOUT_MS);
    return () => clearStallTimer();
  }, [activeImageUrl, imgFailed, imgLoading, imageCandidateIndex]);

  useEffect(() => {
    if (!isPreviewOpen) return undefined;

    const syncAnchorRect = () => updateAnchorRect();
    syncAnchorRect();

    window.addEventListener('resize', syncAnchorRect);
    window.addEventListener('scroll', syncAnchorRect, true);

    return () => {
      window.removeEventListener('resize', syncAnchorRect);
      window.removeEventListener('scroll', syncAnchorRect, true);
    };
  }, [isPreviewOpen, updateAnchorRect]);

  const handleCardClick = () => { if (onClick && safeMovie) onClick(safeMovie); };

  const closePreviewImmediately = useCallback(() => {
    clearPreviewTimer();
    popupHoverRef.current = false;
    setIsPreviewOpen(false);
  }, [clearPreviewTimer]);

  const handlePopupOpen = useCallback(() => {
    closePreviewImmediately();
    if (onClick && safeMovie) {
      onClick(safeMovie);
    }
  }, [closePreviewImmediately, onClick, safeMovie]);

  const isInsidePopup = useCallback((target) => (
    target instanceof Element && Boolean(target.closest('[data-movie-card-popup="true"], .movie-card__popup'))
  ), []);

  const isInsideMedia = useCallback((target) => (
    Boolean(mediaRef.current && target instanceof Node && mediaRef.current.contains(target))
  ), []);

  const previewDelayMs = popupVariant === 'inline'
    ? INLINE_HOVER_PREVIEW_DELAY_MS
    : HOVER_PREVIEW_DELAY_MS;

  const beginPreviewDelay = () => {
    clearPreviewTimer();
    previewTimerRef.current = setTimeout(() => {
      updateAnchorRect();
      setIsPreviewOpen(true);
    }, previewDelayMs);
  };

  const handleMediaMouseLeave = useCallback((event) => {
    clearPreviewTimer();
    const nextTarget = event.relatedTarget;
    if (popupHoverRef.current || isInsidePopup(nextTarget)) return;
    closePreviewImmediately();
  }, [clearPreviewTimer, closePreviewImmediately, isInsidePopup]);

  const handlePopupEnter = useCallback(() => {
    popupHoverRef.current = true;
    clearPreviewTimer();
  }, [clearPreviewTimer]);

  const handlePopupLeave = useCallback((event) => {
    popupHoverRef.current = false;
    const nextTarget = event?.relatedTarget;
    if (isInsideMedia(nextTarget)) return;
    closePreviewImmediately();
  }, [closePreviewImmediately, isInsideMedia]);

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
    if (!userId) { showLoginTooltip('Login to save this movie'); return; }
    if (onToggleWatchlist && safeMovie) {
      try { await onToggleWatchlist(safeMovie); } catch (err) { console.error('Watchlist update failed:', err); }
    }
  };

  const handleWatchedClick = async (event) => {
    event.stopPropagation();
    if (!userId) { showLoginTooltip('Login to mark movies as watched'); return; }
    if (onToggleWatched && safeMovie) {
      try { await onToggleWatched(safeMovie); } catch (err) { console.error('Watched update failed:', err); }
    }
  };

  if (!safeMovie || Number.isNaN(movieId)) return null;

  return (
    <>
      <div
        ref={cardRef}
        className={`movie-card ${popupVariant === 'inline' ? 'movie-card--popup-inline' : ''} ${isPreviewOpen ? 'movie-card--preview' : ''}`}
        onClick={handleCardClick}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => e.key === 'Enter' && handleCardClick()}
        id={`movie-card-${safeMovie.movie_id}`}
      >
        {showTooltip && (
          <div className="movie-card__watchlist-tooltip">{tooltipText}</div>
        )}

        {activeImageUrl && !imgFailed ? (
          <div
            ref={mediaRef}
            className="movie-card__media"
            onMouseEnter={beginPreviewDelay}
            onMouseLeave={handleMediaMouseLeave}
          >
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
              onLoad={() => { clearStallTimer(); setImgLoading(false); }}
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

        {/* Language badge */}
        {isHindi && (
          <span className="movie-card__lang-badge movie-card__lang-badge--hi">हिंदी</span>
        )}
        {!isHindi && safeMovie.original_language && safeMovie.original_language !== 'en' && (
          <span className="movie-card__lang-badge">{safeMovie.original_language}</span>
        )}

        {popupVariant === 'inline' && (
          <div
            className="movie-card__popup"
            onClick={(event) => {
              event.stopPropagation();
              handleCardClick();
            }}
            onMouseEnter={handlePopupEnter}
            onMouseLeave={handlePopupLeave}
            onFocusCapture={handlePopupEnter}
            onBlurCapture={(event) => {
              if (event.currentTarget.contains(event.relatedTarget)) return;
              handlePopupLeave(event);
            }}
          >
            <div className="movie-card__popup-side-actions">
              <button
                className={`movie-card__popup-circle-btn${isInWatchlist ? ' active' : ''}`}
                onClick={handleWatchlistClick}
                aria-label={isInWatchlist ? 'Remove from watchlist' : 'Add to watchlist'}
              >
                {isInWatchlist ? '✓' : '+'}
              </button>
              <button
                className="movie-card__popup-circle-btn"
                onClick={handleWatchedClick}
                aria-label={isWatched ? 'Mark unwatched' : 'Mark watched'}
              >
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13">
                  <path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3zM7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3" />
                </svg>
              </button>
            </div>

            <div className="movie-card__popup-body">
              <div className="movie-card__title-sleek">{safeMovie.title}</div>

              <div className="movie-card__popup-metadata-row">
                <span className="movie-card__match-score">
                  {safeMovie.vote_average ? `${Math.round(safeMovie.vote_average * 10)}% match` : ''}
                </span>
                <span className="movie-card__age-rating">U/A 16+</span>
                {(safeMovie.runtime || safeMovie.duration) && (
                  <span className="movie-card__duration">{safeMovie.runtime || safeMovie.duration} min</span>
                )}
                <span className="movie-card__hd-badge">HD</span>
              </div>

              {safeMovie.genres && (
                <div className="movie-card__popup-genres-row">
                  {String(safeMovie.genres).split('|').join(' • ')}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Portal popup — renders into document.body, escapes all overflow clips */}
      {popupVariant === 'detached' && (
        <PopupPortal
          movie={safeMovie}
          anchorRect={anchorRect}
          isVisible={isPreviewOpen}
          watchlist={watchlist}
          watched={watched}
          onToggleWatchlist={handleWatchlistClick}
          onToggleWatched={handleWatchedClick}
          userId={userId}
          onOpen={handlePopupOpen}
          popupImageUrl={popupImageUrl}
          onPopupEnter={handlePopupEnter}
          onPopupLeave={handlePopupLeave}
        />
      )}
    </>
  );
}

function areCardPropsEqual(prevProps, nextProps) {
  const prevMovie = prevProps.movie;
  const nextMovie = nextProps.movie;
  const prevMovieId = Number(prevMovie?.movie_id);
  const nextMovieId = Number(nextMovie?.movie_id);

  if (prevMovieId !== nextMovieId) return false;
  if (!prevMovie || !nextMovie) return prevMovie === nextMovie;

  if (
    prevMovie.title !== nextMovie.title
    || prevMovie.vote_average !== nextMovie.vote_average
    || prevMovie.year !== nextMovie.year
    || prevMovie.genres !== nextMovie.genres
    || prevMovie.original_language !== nextMovie.original_language
    || prevMovie.hero_image_url !== nextMovie.hero_image_url
    || prevMovie.card_image_url !== nextMovie.card_image_url
    || prevMovie.backdrop_url !== nextMovie.backdrop_url
    || prevMovie.poster_url !== nextMovie.poster_url
  ) return false;

  if (prevProps.userId !== nextProps.userId) return false;
  if (prevProps.imageLoading !== nextProps.imageLoading) return false;
  if (prevProps.imageFetchPriority !== nextProps.imageFetchPriority) return false;
  if (prevProps.popupVariant !== nextProps.popupVariant) return false;

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
