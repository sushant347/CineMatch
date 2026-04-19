import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

const HERO_IMAGE_LOAD_TIMEOUT_MS = 3200;
const HERO_MAX_RETRIES_PER_CANDIDATE = 1;
const LANGUAGE_NAME_FALLBACK = {
  en: 'English',
  hi: 'Hindi',
  ja: 'Japanese',
  ko: 'Korean',
  fr: 'French',
  es: 'Spanish',
  de: 'German',
  zh: 'Chinese',
  ta: 'Tamil',
  te: 'Telugu',
  ml: 'Malayalam',
  bn: 'Bengali',
};

function buildHeroResponsiveSource(url) {
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
  return {
    src: `${base}/t/p/w1280/${assetPath}`,
    srcSet: [
      `${base}/t/p/w500/${assetPath} 500w`,
      `${base}/t/p/w780/${assetPath} 780w`,
      `${base}/t/p/w1280/${assetPath} 1280w`,
    ].join(', '),
    sizes: '100vw',
  };
}

function toFullLanguageName(languageCode) {
  const normalizedCode = String(languageCode || '').trim().toLowerCase();
  if (!normalizedCode) return '';

  const fallbackName = LANGUAGE_NAME_FALLBACK[normalizedCode] || normalizedCode.toUpperCase();
  try {
    if (typeof Intl !== 'undefined' && typeof Intl.DisplayNames === 'function') {
      const languageDisplay = new Intl.DisplayNames(['en'], { type: 'language' });
      const resolvedName = languageDisplay.of(normalizedCode);
      if (resolvedName) {
        return resolvedName;
      }
    }
  } catch {
    // Ignore and use fallback below.
  }

  return fallbackName;
}

export default function Hero({ movie, movies = [], onMoreInfo, userId = null }) {
  const heroMovies = useMemo(() => {
    if (Array.isArray(movies) && movies.length > 0) {
      return movies.filter(Boolean);
    }
    return movie ? [movie] : [];
  }, [movie, movies]);

  const [activeIndex, setActiveIndex] = useState(0);
  const [heroImageIndex, setHeroImageIndex] = useState(0);
  const [heroCandidateRetries, setHeroCandidateRetries] = useState(0);
  const [heroCacheBustKey, setHeroCacheBustKey] = useState(0);
  const [backdropLoading, setBackdropLoading] = useState(true);
  const [backdropFailed, setBackdropFailed] = useState(false);
  const [isTransitioning, setIsTransitioning] = useState(false);
  const loadTimeoutRef = useRef(null);
  const loadedHeroUrlsRef = useRef(new Set());

  useEffect(() => {
    setActiveIndex(0);
  }, [heroMovies.length]);

  useEffect(() => {
    if (heroMovies.length <= 1) return undefined;

    const intervalId = window.setInterval(() => {
      setIsTransitioning(true);
      setTimeout(() => {
        setActiveIndex((prev) => (prev + 1) % heroMovies.length);
        setIsTransitioning(false);
      }, 400);
    }, 8000);

    return () => window.clearInterval(intervalId);
  }, [heroMovies.length]);

  const handleIndicatorClick = (index) => {
    setIsTransitioning(true);
    setTimeout(() => {
      setActiveIndex(index);
      setIsTransitioning(false);
    }, 300);
  };

  const activeMovie = heroMovies[activeIndex] || null;
  const heroTagline = String(activeMovie?.tagline || '').trim();
  const heroOverview = String(activeMovie?.overview || '').trim();
  const normalizedTitle = String(activeMovie?.title || '').trim().toLowerCase();
  const compactNormalizedTitle = normalizedTitle.replace(/[^a-z0-9]+/g, ' ').trim();
  const isOppenheimerHero = compactNormalizedTitle.includes('oppenheimer');
  const isSpiderManHero = compactNormalizedTitle.includes('spider man')
    || compactNormalizedTitle.includes('spiderman');
  const heroDescription = heroOverview
    || `Discover ${activeMovie?.title || 'this feature'} and dive into more top picks curated for you.`;
  const heroGenres = activeMovie?.genres
    ? String(activeMovie.genres).split('|').filter(Boolean).slice(0, 4)
    : [];
  const languageLabel = useMemo(
    () => toFullLanguageName(activeMovie?.original_language),
    [activeMovie?.original_language],
  );

  const heroImageCandidates = useMemo(() => {
    if (!activeMovie) return [];

    const landscapeUrls = [
      activeMovie.hero_image_url,
      activeMovie.backdrop_url,
    ].filter(Boolean);
    const fallbackUrls = [
      activeMovie.poster_url,
      activeMovie.card_image_url,
    ].filter(Boolean);

    const candidates = landscapeUrls.length > 0 ? landscapeUrls : fallbackUrls;

    const uniqueByUrl = [];
    const seen = new Set();
    for (const candidate of candidates) {
      if (!candidate || seen.has(candidate)) continue;
      seen.add(candidate);
      uniqueByUrl.push(candidate);
    }

    return uniqueByUrl;
  }, [
    activeMovie?.hero_image_url,
    activeMovie?.backdrop_url,
    activeMovie?.poster_url,
    activeMovie?.card_image_url,
  ]);

  const rawBackdropUrl = heroImageCandidates[heroImageIndex] || '';

  const backdropUrl = useMemo(() => {
    if (!rawBackdropUrl) return '';
    if (heroCacheBustKey <= 0 || !/^https?:\/\//i.test(rawBackdropUrl)) {
      return rawBackdropUrl;
    }
    const separator = rawBackdropUrl.includes('?') ? '&' : '?';
    return `${rawBackdropUrl}${separator}r=${heroCacheBustKey}`;
  }, [rawBackdropUrl, heroCacheBustKey]);

  const responsiveBackdrop = useMemo(
    () => buildHeroResponsiveSource(backdropUrl),
    [backdropUrl],
  );

  const clearLoadTimeout = useCallback(() => {
    if (loadTimeoutRef.current) {
      clearTimeout(loadTimeoutRef.current);
      loadTimeoutRef.current = null;
    }
  }, []);

  const retryCurrentHeroImage = useCallback(() => {
    setHeroCandidateRetries((prev) => prev + 1);
    setHeroCacheBustKey((prev) => prev + 1);
    setBackdropLoading(true);
    setBackdropFailed(false);
  }, []);

  const advanceHeroImage = useCallback(() => {
    if (heroImageIndex < heroImageCandidates.length - 1) {
      setHeroImageIndex((prev) => prev + 1);
      setHeroCandidateRetries(0);
      setHeroCacheBustKey(0);
      setBackdropLoading(true);
      setBackdropFailed(false);
      return;
    }
    setBackdropLoading(false);
    setBackdropFailed(true);
  }, [heroImageIndex, heroImageCandidates.length]);

  useEffect(() => {
    // Always start each movie on its primary hero image (first candidate).
    setHeroImageIndex(0);

    setHeroCandidateRetries(0);
    setHeroCacheBustKey(0);
    setBackdropLoading(true);
    setBackdropFailed(false);
    clearLoadTimeout();
  }, [activeMovie?.movie_id, activeIndex, heroImageCandidates, clearLoadTimeout]);

  useEffect(() => {
    if (!backdropUrl) {
      setBackdropLoading(false);
      setBackdropFailed(true);
      clearLoadTimeout();
      return;
    }
    const alreadyLoaded = loadedHeroUrlsRef.current.has(rawBackdropUrl);
    setBackdropLoading(!alreadyLoaded);
    setBackdropFailed(false);
  }, [backdropUrl, rawBackdropUrl, clearLoadTimeout]);

  useEffect(() => {
    if (!backdropUrl || backdropFailed || !backdropLoading) {
      clearLoadTimeout();
      return undefined;
    }
    clearLoadTimeout();
    loadTimeoutRef.current = setTimeout(() => {
      if (heroCandidateRetries < HERO_MAX_RETRIES_PER_CANDIDATE) {
        retryCurrentHeroImage();
        return;
      }
      advanceHeroImage();
    }, HERO_IMAGE_LOAD_TIMEOUT_MS);
    return () => clearLoadTimeout();
  }, [
    backdropUrl, backdropFailed, backdropLoading, heroCandidateRetries,
    rawBackdropUrl, retryCurrentHeroImage, advanceHeroImage, clearLoadTimeout,
  ]);

  useEffect(() => {
    if (heroMovies.length <= 1) return;
    const nextMovie = heroMovies[(activeIndex + 1) % heroMovies.length];
    if (!nextMovie) return;
    const candidate = nextMovie.hero_image_url || nextMovie.backdrop_url
      || nextMovie.poster_url || nextMovie.card_image_url;
    if (!candidate) return;
    const nextImage = buildHeroResponsiveSource(candidate);
    const img = new Image();
    img.decoding = 'async';
    img.src = nextImage.src || candidate;
  }, [heroMovies, activeIndex]);

  useEffect(() => () => clearLoadTimeout(), [clearLoadTimeout]);

  if (!activeMovie) {
    return <div className="hero skeleton skeleton--hero" />;
  }

  // Only show match score if user is logged in AND has a rating history
  const matchScore = userId && activeMovie.vote_average > 0
    ? Math.min(99, Math.round(activeMovie.vote_average * 9.5))
    : null;

  return (
    <section className="hero" id="hero-banner">
      {/* Background layer */}
      <div className="hero__media-shell">
        {backdropUrl && !backdropFailed && (
          <img
            key={`hero-backdrop-${activeMovie.movie_id}-${heroImageIndex}`}
            className={`hero__backdrop hero__backdrop--animated ${isOppenheimerHero ? 'hero__backdrop--oppenheimer' : ''} ${isSpiderManHero ? 'hero__backdrop--spiderman' : ''} ${backdropLoading ? 'is-loading' : ''} ${isTransitioning ? 'is-transitioning' : ''}`}
            src={responsiveBackdrop.src || backdropUrl}
            srcSet={responsiveBackdrop.srcSet}
            sizes={responsiveBackdrop.sizes}
            alt={activeMovie.title}
            loading="eager"
            decoding="async"
            fetchPriority="high"
            referrerPolicy="no-referrer"
            onLoad={() => {
              clearLoadTimeout();
              if (rawBackdropUrl) {
                loadedHeroUrlsRef.current.add(rawBackdropUrl);
              }
              setBackdropLoading(false);
              setBackdropFailed(false);
            }}
            onError={() => {
              clearLoadTimeout();
              if (heroCandidateRetries < HERO_MAX_RETRIES_PER_CANDIDATE) {
                retryCurrentHeroImage();
                return;
              }
              advanceHeroImage();
            }}
          />
        )}
        {(!backdropUrl || backdropLoading || backdropFailed) && (
          <div className="skeleton hero__backdrop-skeleton" />
        )}
      </div>

      {/* Multi-layer cinematic gradient */}
      <div className="hero__gradient" />
      <div className="hero__gradient-side" />
      <div className="hero__gradient-bottom" />

      {/* Content */}
      <div className={`hero__content ${isTransitioning ? 'hero__content--transitioning' : ''}`}>
        {/* Top badges row */}
        <div className="hero__badges">
          <span className="hero__badge hero__badge--featured">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
            Top Pick
          </span>
          {activeMovie.year && (
            <span className="hero__badge hero__badge--year">{activeMovie.year}</span>
          )}
          {languageLabel && (
            <span className="hero__badge hero__badge--lang">
              {languageLabel}
            </span>
          )}
          {matchScore && (
            <span className="hero__badge hero__badge--match">{matchScore}% Match</span>
          )}
        </div>

        {/* Movie title */}
        <h1 className="hero__title" key={`title-${activeMovie.movie_id}`}>
          {activeMovie.title}
        </h1>

        {/* Tagline */}
        {heroTagline && (
          <p className="hero__tagline" key={`tagline-${activeMovie.movie_id}`}>
            "{heroTagline}"
          </p>
        )}

        {/* Overview */}
        <p className="hero__overview" key={`overview-${activeMovie.movie_id}`}>
          {heroDescription}
        </p>

        {/* Genre chips */}
        {heroGenres.length > 0 && (
          <div className="hero__genres">
            {heroGenres.map((genre) => (
              <span key={genre} className="hero__genre-chip">{genre}</span>
            ))}
          </div>
        )}

        {/* Rating row */}
        {activeMovie.vote_average > 0 && (
          <div className="hero__rating-row">
            <span className="hero__stars">
              {Array.from({ length: 5 }).map((_, i) => (
                <svg
                  key={i}
                  className={`hero__star ${i < Math.round(activeMovie.vote_average / 2) ? 'hero__star--filled' : ''}`}
                  width="14" height="14" viewBox="0 0 24 24" fill="currentColor"
                >
                  <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
                </svg>
              ))}
            </span>
            <span className="hero__rating-value">{activeMovie.vote_average.toFixed(1)}</span>
            <span className="hero__rating-sep">·</span>
            <span className="hero__rating-label">Community Rating</span>
          </div>
        )}

        {/* Action button */}
        <div className="hero__actions">
          <button
            className="hero__cta-btn"
            onClick={() => onMoreInfo(activeMovie)}
            id="hero-info-btn"
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
            More Info
          </button>
          <button
            className="hero__watchlist-btn"
            onClick={() => onMoreInfo(activeMovie)}
            id="hero-watchlist-btn"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Watchlist
          </button>
        </div>

        {/* Carousel indicators */}
        {heroMovies.length > 1 && (
          <div className="hero__indicators" aria-label="Hero movie carousel indicators">
            {heroMovies.map((heroItem, index) => (
              <button
                key={`hero-indicator-${heroItem.movie_id}`}
                className={`hero__indicator ${index === activeIndex ? 'active' : ''}`}
                onClick={() => handleIndicatorClick(index)}
                aria-label={`Show ${heroItem.title}`}
              />
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
