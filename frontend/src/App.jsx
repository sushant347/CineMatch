import {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
  Suspense,
  lazy,
} from 'react';
import Navbar from './components/Navbar';
import Hero from './components/Hero';
import MovieRow from './components/MovieRow';
import MovieCard from './components/MovieCard';
import {
  getColdStartRecs,
  getHeroMovies,
  getHomeData,
  getGenreMovies,
  getLanguageMovies,
  getWatchlist,
  toggleWatchlist,
  getUserProfile,
  loginUser,
  registerUser,
  logoutUser,
  getAuthMe,
  getAuthPreferences,
  saveAuthPreferences,
  initializeAuthSession,
  getWatchedMovies,
  toggleWatched,
  getUserRatings,
} from './api/client';


const MovieModal = lazy(() => import('./components/MovieModal'));
const AuthModal = lazy(() => import('./components/AuthModal'));
const ColdStartModal = lazy(() => import('./components/ColdStartModal'));
const ProfilePage = lazy(() => import('./components/ProfilePage'));

const BLOCKED_MOVIE_TITLE_PATTERNS = [
  'everything everywhere all at once',
];

function normalizeMovieTitle(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function toMovieFromAny(value) {
  if (!value || typeof value !== 'object') return null;
  if (value.movie && typeof value.movie === 'object') return value.movie;
  return value;
}

function isBlockedMovie(value) {
  const movie = toMovieFromAny(value);
  const normalizedTitle = normalizeMovieTitle(movie?.title);
  if (!normalizedTitle) return false;
  return BLOCKED_MOVIE_TITLE_PATTERNS.some(
    (pattern) => normalizedTitle === pattern || normalizedTitle.includes(pattern),
  );
}

function filterBlockedMovieItems(items) {
  if (!Array.isArray(items)) return [];
  return items.filter((item) => !isBlockedMovie(item));
}

function movieIdFromAny(value) {
  const movie = toMovieFromAny(value);
  const movieId = Number(movie?.movie_id);
  return Number.isNaN(movieId) ? null : movieId;
}


function watchlistStorageKey(userId) {
  return `cinematch_watchlist_${userId}`;
}


function readWatchlistCache(userId) {
  if (!userId) return { ids: new Set(), movies: [] };
  try {
    const raw = localStorage.getItem(watchlistStorageKey(userId));
    if (!raw) return { ids: new Set(), movies: [] };
    const parsed = JSON.parse(raw);
    const movies = dedupeMovies(Array.isArray(parsed.movies) ? parsed.movies : []);
    const blockedMovieIds = new Set(
      (Array.isArray(parsed.movies) ? parsed.movies : [])
        .filter((item) => isBlockedMovie(item))
        .map((item) => movieIdFromAny(item))
        .filter((id) => id !== null),
    );
    const ids = new Set(
      (parsed.ids || [])
        .map((id) => Number(id))
        .filter((id) => !Number.isNaN(id) && !blockedMovieIds.has(id)),
    );
    return { ids, movies };
  } catch {
    return { ids: new Set(), movies: [] };
  }
}


function writeWatchlistCache(userId, ids, movies) {
  if (!userId) return;
  const rawMovies = Array.isArray(movies) ? movies : [];
  const blockedMovieIds = new Set(
    rawMovies
      .filter((item) => isBlockedMovie(item))
      .map((item) => movieIdFromAny(item))
      .filter((id) => id !== null),
  );
  const sanitizedMovies = dedupeMovies(rawMovies);
  const payload = {
    ids: Array.from(ids)
      .map((id) => Number(id))
      .filter((id) => !Number.isNaN(id) && !blockedMovieIds.has(id)),
    movies: sanitizedMovies,
  };
  localStorage.setItem(watchlistStorageKey(userId), JSON.stringify(payload));
}


function watchedStorageKey(userId) {
  return `cinematch_watched_${userId}`;
}


function readWatchedCache(userId) {
  if (!userId) return new Set();
  try {
    const raw = localStorage.getItem(watchedStorageKey(userId));
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set((parsed.ids || []).map((id) => Number(id)));
  } catch {
    return new Set();
  }
}


function writeWatchedCache(userId, ids) {
  if (!userId) return;
  localStorage.setItem(
    watchedStorageKey(userId),
    JSON.stringify({ ids: Array.from(ids) }),
  );
}

function unwrapMovie(item) {
  return toMovieFromAny(item);
}

function dedupeMovies(items) {
  const deduped = [];
  const seen = new Set();

  for (const item of filterBlockedMovieItems(items || [])) {
    const movie = unwrapMovie(item);
    const movieId = Number(movie?.movie_id);
    if (!movie || Number.isNaN(movieId) || seen.has(movieId)) continue;
    seen.add(movieId);
    deduped.push(movie);
  }

  return deduped;
}

function combineMovieLists(...lists) {
  return dedupeMovies(lists.flat());
}

function sanitizeHomePayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;

  return {
    ...payload,
    trending: filterBlockedMovieItems(payload.trending),
    hindi: filterBlockedMovieItems(payload.hindi),
    recommended: filterBlockedMovieItems(payload.recommended),
    explained: Array.isArray(payload.explained)
      ? payload.explained
        .map((group) => ({
          ...group,
          recommendations: filterBlockedMovieItems(group?.recommendations),
        }))
        .filter((group) => Array.isArray(group.recommendations) && group.recommendations.length > 0)
      : [],
  };
}

const HERO_TOTAL_MOVIES = 8;

function scoreMovie(item) {
  const movie = unwrapMovie(item);
  const voteAverage = Number(movie?.vote_average || 0);
  const popularity = Number(movie?.popularity || 0);
  return (voteAverage * 1000) + popularity;
}

function topMoviesByScore(items, limit = HERO_TOTAL_MOVIES) {
  const uniqueMovies = dedupeMovies(items);
  const moviesWithBackdrop = uniqueMovies.filter(
    (movie) => Boolean(movie?.hero_image_url || movie?.backdrop_url),
  );
  const sourcePool = moviesWithBackdrop.length >= limit ? moviesWithBackdrop : uniqueMovies;

  return sourcePool
    .sort((left, right) => scoreMovie(right) - scoreMovie(left))
    .slice(0, limit);
}

function hasExplicitPreference(genres, languages) {
  const safeGenres = Array.isArray(genres) ? genres.filter(Boolean) : [];
  const safeLanguages = Array.isArray(languages) ? languages.filter(Boolean) : [];
  const nonDefaultLanguages = safeLanguages.filter(
    (lang) => String(lang).toLowerCase() !== 'en',
  );
  return safeGenres.length > 0 || nonDefaultLanguages.length > 0;
}

function ChunkFallback({ compact = false, modal = false }) {
  return (
    <div className={`chunk-fallback ${compact ? 'chunk-fallback--compact' : ''} ${modal ? 'chunk-fallback--modal' : ''}`}>
      <div className="skeleton chunk-fallback__panel" />
      {!compact && <div className="skeleton chunk-fallback__panel chunk-fallback__panel--secondary" />}
    </div>
  );
}

export default function App() {
  const [selectedUser, setSelectedUser] = useState(null);
  const [homeData, setHomeData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedMovie, setSelectedMovie] = useState(null);
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [showPreferenceModal, setShowPreferenceModal] = useState(false);
  const [authLoading, setAuthLoading] = useState(false);
  const [searchResults, setSearchResults] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [watchlist, setWatchlist] = useState(new Set());
  const [watchlistMovies, setWatchlistMovies] = useState([]);
  const [watched, setWatched] = useState(new Set());
  const [userRatings, setUserRatings] = useState({});
  const [preferredGenres, setPreferredGenres] = useState([]);
  const [preferredLanguages, setPreferredLanguages] = useState(['en']);
  const [preferenceMovies, setPreferenceMovies] = useState([]);
  const [preferenceLoading, setPreferenceLoading] = useState(false);
  const [genreRows, setGenreRows] = useState({
    comedy: [],
    anime: [],
    crimeThriller: [],
    actionHorrorMystery: [],
  });
  const [genreRowsLoading, setGenreRowsLoading] = useState(false);
  const [profileData, setProfileData] = useState(null);
  const [profileLoading, setProfileLoading] = useState(false);
  const [personalizeNotice, setPersonalizeNotice] = useState('');
  const [heroGenreMovies, setHeroGenreMovies] = useState([]);
  const modalImageWarmCacheRef = useRef(new Set());
  const rowImageWarmCacheRef = useRef(new Set());
  const [currentView, setCurrentView] = useState(() => (
    window.location.pathname === '/profile' ? 'profile' : 'home'
  ));
  const recommendationUserId = selectedUser?.user_id || null;
  const authUserId = selectedUser?.is_custom ? selectedUser.user_id : null;
  const isAuthenticated = Boolean(authUserId);
  const isProfileView = currentView === 'profile' && Boolean(recommendationUserId);

  const loadPreferenceMovies = useCallback(async (genres, languages) => {
    if (!authUserId) {
      setPreferenceMovies([]);
      setPreferenceLoading(false);
      return [];
    }

    const safeGenres = Array.isArray(genres) ? genres.filter(Boolean) : [];
    const safeLanguagesRaw = Array.isArray(languages) ? languages.filter(Boolean) : [];
    const safeLanguages = safeLanguagesRaw.length > 0 ? safeLanguagesRaw : ['en'];

    setPreferenceLoading(true);
    try {
      const picks = await getColdStartRecs(safeGenres, safeLanguages);
      setPreferenceMovies(Array.isArray(picks) ? picks : []);
      return Array.isArray(picks) ? picks : [];
    } catch (err) {
      console.error('Failed to load preference picks:', err);
      setPreferenceMovies([]);
      return [];
    } finally {
      setPreferenceLoading(false);
    }
  }, [authUserId]);

  useEffect(() => {
    let active = true;

    const restoreSession = async () => {
      await initializeAuthSession();
      try {
        const payload = await getAuthMe();
        if (!active) return;
        setSelectedUser({
          user_id: payload.user_id,
          user_name: payload.user_name,
          rating_count: payload.rating_count,
          avg_rating: payload.avg_rating,
          is_custom: true,
        });
      } catch (err) {
        if (!active) return;
        if (err?.status !== 401) {
          console.error('Failed to restore auth session:', err);
        }
      }
    };

    restoreSession();
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!authUserId) {
      setPreferredGenres([]);
      setPreferredLanguages(['en']);
      setPreferenceMovies([]);
      setPreferenceLoading(false);
      setShowPreferenceModal(false);
      return;
    }

    let active = true;
    getAuthPreferences()
      .then((payload) => {
        if (!active) return;

        const genres = Array.isArray(payload?.genres) ? payload.genres.filter(Boolean) : [];
        const languagesRaw = Array.isArray(payload?.languages) ? payload.languages.filter(Boolean) : [];
        const languages = languagesRaw.length > 0 ? languagesRaw : ['en'];
        const explicitPreference = hasExplicitPreference(genres, languagesRaw);

        setPreferredGenres(genres);
        setPreferredLanguages(languages);

        if (explicitPreference) {
          loadPreferenceMovies(genres, languages);
        } else {
          setPreferenceMovies([]);
          setPreferenceLoading(false);
        }
      })
      .catch((err) => {
        if (!active) return;
        console.error('Failed to load saved preferences:', err);

        if (err?.status === 401) {
          setSelectedUser(null);
        }

        setPreferredGenres([]);
        setPreferredLanguages(['en']);
        setPreferenceMovies([]);
        setPreferenceLoading(false);
      });

    return () => {
      active = false;
    };
  }, [authUserId, loadPreferenceMovies]);

  // Fetch home data
  const fetchHome = useCallback(async (uid) => {
    setLoading(true);
    try {
      const data = await getHomeData(uid);
      const safeData = sanitizeHomePayload(data);
      setHomeData(safeData);
      return safeData;
    } catch (err) {
      console.error('Failed to load home data:', err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHome(recommendationUserId);
  }, [recommendationUserId, fetchHome]);

  useEffect(() => {
    let active = true;

    getHeroMovies(HERO_TOTAL_MOVIES)
      .then((movies) => {
        if (!active) return;
        setHeroGenreMovies(dedupeMovies(movies));
      })
      .catch((err) => {
        if (!active) return;
        console.error('Failed to load hero picks:', err);
        setHeroGenreMovies([]);
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!homeData) {
      return undefined;
    }

    let active = true;
    setGenreRowsLoading(true);

    // Phase 1: Load Comedy immediately (first visible genre row — user sees it fast)
    getGenreMovies('Comedy', 20)
      .then((comedyMovies) => {
        if (!active) return;
        setGenreRows((prev) => ({ ...prev, comedy: dedupeMovies(comedyMovies) }));
        setGenreRowsLoading(false);
      })
      .catch(() => {
        if (!active) return;
        setGenreRowsLoading(false);
      });

    // Phase 2: Load remaining genre rows in background (deferred)
    const loadRemainingGenres = () => {
      Promise.all([
        getGenreMovies('Animation', 20),
        getLanguageMovies('ja', 24),
        getGenreMovies('Crime', 16),
        getGenreMovies('Thriller', 16),
        getGenreMovies('Action', 16),
        getGenreMovies('Horror', 16),
        getGenreMovies('Mystery', 16),
      ])
        .then(([
          animationMovies,
          japaneseMovies,
          crimeMovies,
          thrillerMovies,
          actionMovies,
          horrorMovies,
          mysteryMovies,
        ]) => {
          if (!active) return;

          const animationList = dedupeMovies(animationMovies);
          const japaneseByLanguage = dedupeMovies(japaneseMovies);
          const japaneseAnimation = japaneseByLanguage.filter(
            (movie) => String(movie?.genres || '').toLowerCase().includes('animation'),
          );
          const japaneseFromAnimation = animationList.filter(
            (movie) => String(movie?.original_language || '').toLowerCase() === 'ja',
          );
          const animeRow = combineMovieLists(
            japaneseAnimation,
            japaneseByLanguage,
            japaneseFromAnimation,
          );

          setGenreRows((prev) => ({
            ...prev,
            anime: animeRow.length > 0 ? animeRow : animationList,
            crimeThriller: combineMovieLists(crimeMovies, thrillerMovies),
            actionHorrorMystery: combineMovieLists(actionMovies, horrorMovies, mysteryMovies),
          }));
        })
        .catch((err) => {
          if (!active) return;
          console.error('Failed to load secondary genre sections:', err);
        });
    };

    // Start secondary genres after 200ms — after first paint settles
    const timeoutId = window.setTimeout(loadRemainingGenres, 200);
    return () => {
      active = false;
      window.clearTimeout(timeoutId);
    };
  }, [homeData, recommendationUserId]);

  useEffect(() => {
    if (!authUserId) {
      setWatchlist(new Set());
      setWatchlistMovies([]);
      return;
    }

    const cached = readWatchlistCache(authUserId);
    setWatchlist(cached.ids);
    setWatchlistMovies(cached.movies);

    let active = true;
    getWatchlist(authUserId)
      .then((movies) => {
        if (!active) return;
        const safeMovies = dedupeMovies(movies);
        const nextSet = new Set(safeMovies.map((movie) => Number(movie.movie_id)));
        setWatchlist(nextSet);
        setWatchlistMovies(safeMovies);
        writeWatchlistCache(authUserId, nextSet, safeMovies);
      })
      .catch((err) => {
        console.error('Failed to load watchlist:', err);
      });

    return () => {
      active = false;
    };
  }, [authUserId]);

  useEffect(() => {
    if (!authUserId) {
      setWatched(new Set());
      return;
    }

    const cached = readWatchedCache(authUserId);
    setWatched(cached);

    let active = true;
    getWatchedMovies(authUserId)
      .then((payload) => {
        if (!active) return;
        const nextSet = new Set((payload?.movie_ids || []).map((movieId) => Number(movieId)));
        setWatched(nextSet);
        writeWatchedCache(authUserId, nextSet);
      })
      .catch((err) => {
        console.error('Failed to load watched movies:', err);
      });

    return () => {
      active = false;
    };
  }, [authUserId]);

  useEffect(() => {
    if (!authUserId) {
      setUserRatings({});
      return;
    }

    let active = true;
    getUserRatings(authUserId)
      .then((ratings) => {
        if (!active) return;
        const nextRatings = {};
        for (const item of ratings || []) {
          const movieId = Number(item?.movie_id);
          const rating = Number(item?.rating);
          if (Number.isNaN(movieId) || Number.isNaN(rating)) continue;
          nextRatings[movieId] = rating;
        }
        setUserRatings(nextRatings);
      })
      .catch((err) => {
        if (!active) return;
        console.error('Failed to load user ratings:', err);
        setUserRatings({});
      });

    return () => {
      active = false;
    };
  }, [authUserId]);

  useEffect(() => {
    if (!isProfileView || !recommendationUserId) {
      setProfileData(null);
      setProfileLoading(false);
      return;
    }

    let active = true;
    setProfileLoading(true);

    getUserProfile(recommendationUserId)
      .then((data) => {
        if (active) setProfileData(data);
      })
      .catch((err) => {
        console.error('Failed to load profile:', err);
        if (active) setProfileData(null);
      })
      .finally(() => {
        if (active) setProfileLoading(false);
      });

    return () => {
      active = false;
    };
  }, [isProfileView, recommendationUserId]);

  useEffect(() => {
    const onPopState = () => {
      const pathView = window.location.pathname === '/profile' ? 'profile' : 'home';
      if (pathView === 'profile' && !recommendationUserId) {
        setCurrentView('home');
        window.history.replaceState({}, '', '/');
      } else {
        setCurrentView(pathView);
      }
    };

    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [recommendationUserId]);

  useEffect(() => {
    if (currentView === 'profile' && !recommendationUserId) {
      setCurrentView('home');
      window.history.replaceState({}, '', '/');
    }
  }, [currentView, recommendationUserId]);

  const handleNavigate = useCallback((view) => {
    const nextView = view === 'profile' && recommendationUserId ? 'profile' : 'home';
    setCurrentView(nextView);
    const targetPath = nextView === 'profile' ? '/profile' : '/';
    if (window.location.pathname !== targetPath) {
      window.history.pushState({}, '', targetPath);
    }
  }, [recommendationUserId]);

  const warmMovieImage = useCallback((movie) => {
    const imageUrl = movie?.hero_image_url
      || movie?.backdrop_url
      || movie?.poster_url
      || movie?.card_image_url;

    if (!imageUrl || modalImageWarmCacheRef.current.has(imageUrl)) {
      return;
    }

    modalImageWarmCacheRef.current.add(imageUrl);
    const img = new Image();
    img.decoding = 'async';
    img.src = imageUrl;
  }, []);

  const warmRowImage = useCallback((movie) => {
    const imageUrl = movie?.card_image_url
      || movie?.poster_url
      || movie?.backdrop_url
      || movie?.hero_image_url;

    if (!imageUrl || rowImageWarmCacheRef.current.has(imageUrl)) {
      return;
    }

    rowImageWarmCacheRef.current.add(imageUrl);
    const img = new Image();
    img.decoding = 'async';
    img.src = imageUrl;
  }, []);

  const handleMovieClick = useCallback((movie) => {
    warmMovieImage(movie);
    setSelectedMovie(movie);
  }, [warmMovieImage]);

  const handleScrollToSections = useCallback(() => {
    const sections = document.getElementById('home-sections');
    if (!sections) return;
    sections.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, []);

  const handleToggleWatchlist = useCallback(async (movie) => {
    if (!authUserId) return false;
    if (isBlockedMovie(movie)) return false;

    const movieId = Number(movie.movie_id);
    const previousSet = new Set(watchlist);
    const previousMovies = [...watchlistMovies];
    const alreadySaved = previousSet.has(movieId);

    const nextSet = new Set(previousSet);
    let nextMovies;
    if (alreadySaved) {
      nextSet.delete(movieId);
      nextMovies = previousMovies.filter((item) => Number(item.movie_id) !== movieId);
    } else {
      nextSet.add(movieId);
      nextMovies = previousMovies.some((item) => Number(item.movie_id) === movieId)
        ? previousMovies
        : [movie, ...previousMovies];
    }

    setWatchlist(nextSet);
    setWatchlistMovies(nextMovies);
    writeWatchlistCache(authUserId, nextSet, nextMovies);

    try {
      await toggleWatchlist(authUserId, movieId);
      return true;
    } catch (err) {
      setWatchlist(previousSet);
      setWatchlistMovies(previousMovies);
      writeWatchlistCache(authUserId, previousSet, previousMovies);
      throw err;
    }
  }, [authUserId, watchlist, watchlistMovies]);

  const handleToggleWatched = useCallback(async (movie) => {
    if (!authUserId) return false;

    const movieId = Number(movie.movie_id);
    const previousSet = new Set(watched);
    const nextSet = new Set(previousSet);

    if (nextSet.has(movieId)) {
      nextSet.delete(movieId);
    } else {
      nextSet.add(movieId);
    }

    setWatched(nextSet);
    writeWatchedCache(authUserId, nextSet);

    try {
      await toggleWatched(authUserId, movieId);
      fetchHome(recommendationUserId);
      return true;
    } catch (err) {
      setWatched(previousSet);
      writeWatchedCache(authUserId, previousSet);
      throw err;
    }
  }, [authUserId, watched, fetchHome, recommendationUserId]);

  const handleAuthSubmit = useCallback(async ({ mode, username, password, confirmPassword }) => {
    setAuthLoading(true);
    try {
      const payload = mode === 'register'
        ? await registerUser(username, password, confirmPassword)
        : await loginUser(username, password);

      setSelectedUser({
        user_id: payload.user_id,
        user_name: payload.user_name,
        rating_count: payload.rating_count,
        avg_rating: payload.avg_rating,
        is_custom: true,
      });
      setCurrentView('home');
      setSearchResults(null);
      setSearchQuery('');
      setSearchLoading(false);
      if (window.location.pathname !== '/') {
        window.history.replaceState({}, '', '/');
      }
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return payload;
    } finally {
      setAuthLoading(false);
    }
  }, []);

  const handleLogout = useCallback(async () => {
    setAuthLoading(true);
    try {
      await logoutUser();
    } catch (err) {
      console.error('Logout failed:', err);
    } finally {
      setAuthLoading(false);
    }

    setSelectedUser(null);
    setShowAuthModal(false);
    setShowPreferenceModal(false);
    setUserRatings({});
    setCurrentView('home');
    setSearchResults(null);
    setSearchQuery('');
    setSearchLoading(false);
    if (window.location.pathname !== '/') {
      window.history.replaceState({}, '', '/');
    }
  }, []);

  const handlePersonalize = useCallback(async () => {
    if (!isAuthenticated) {
      setShowAuthModal(true);
      return;
    }

    if (currentView !== 'home') {
      setCurrentView('home');
      if (window.location.pathname !== '/') {
        window.history.pushState({}, '', '/');
      }
    }

    setSearchResults(null);
    setSearchQuery('');
    setSearchLoading(false);
    window.scrollTo({ top: 0, behavior: 'smooth' });
    setShowPreferenceModal(true);
  }, [isAuthenticated, currentView]);

  useEffect(() => {
    if (!personalizeNotice) return undefined;
    const timeoutId = setTimeout(() => setPersonalizeNotice(''), 2600);
    return () => clearTimeout(timeoutId);
  }, [personalizeNotice]);

  const handlePreferenceChanged = useCallback(() => {
    if (recommendationUserId) {
      fetchHome(recommendationUserId);
    }
  }, [recommendationUserId, fetchHome]);

  const handlePreferenceSubmit = useCallback(async (genres, languages) => {
    const safeGenres = Array.isArray(genres) ? genres.filter(Boolean) : [];
    const safeLanguagesRaw = Array.isArray(languages) ? languages.filter(Boolean) : [];

    try {
      const saved = await saveAuthPreferences(safeGenres, safeLanguagesRaw);
      const nextGenres = Array.isArray(saved?.genres) ? saved.genres.filter(Boolean) : safeGenres;
      const nextLanguagesRaw = Array.isArray(saved?.languages)
        ? saved.languages.filter(Boolean)
        : safeLanguagesRaw;
      const nextLanguages = nextLanguagesRaw.length > 0 ? nextLanguagesRaw : ['en'];
      const explicitPreference = hasExplicitPreference(nextGenres, nextLanguagesRaw);

      setPreferredGenres(nextGenres);
      setPreferredLanguages(nextLanguages);

      let picks = [];

      if (explicitPreference) {
        picks = await loadPreferenceMovies(nextGenres, nextLanguages);
      } else {
        setPreferenceMovies([]);
        setPreferenceLoading(false);
      }

      if (recommendationUserId) {
        fetchHome(recommendationUserId).catch(() => {});
      }

      setShowPreferenceModal(false);
      setPersonalizeNotice(
        explicitPreference
          ? (picks.length > 0 ? 'Preference picks updated.' : 'Preferences saved. Showing fallback picks.')
          : 'Preference picks cleared.',
      );
    } catch (err) {
      if (err?.status === 401) {
        setShowPreferenceModal(false);
        setShowAuthModal(true);
        setSelectedUser(null);
        setPersonalizeNotice('Your session expired. Please log in again.');
        return;
      }

      setPersonalizeNotice(err?.message || 'Unable to save preferences right now.');
    }
  }, [loadPreferenceMovies, recommendationUserId, fetchHome]);

  const handleSearchResults = useCallback((results, query, isLoading = false) => {
    if (Array.isArray(results)) {
      setSearchResults(filterBlockedMovieItems(results));
    } else {
      setSearchResults(results);
    }
    setSearchQuery(query);
    setSearchLoading(Boolean(isLoading));
  }, []);

  const handleRatingSaved = useCallback((movieId, ratingValue) => {
    const safeMovieId = Number(movieId);
    const safeRating = Number(ratingValue);
    if (Number.isNaN(safeMovieId) || Number.isNaN(safeRating)) return;

    setUserRatings((prev) => ({
      ...prev,
      [safeMovieId]: safeRating,
    }));
  }, []);

  const hasPreferenceSelection = useMemo(
    () => hasExplicitPreference(preferredGenres, preferredLanguages),
    [preferredGenres, preferredLanguages],
  );

  const heroMovies = useMemo(() => {
    if (heroGenreMovies.length > 0) {
      return topMoviesByScore(heroGenreMovies, HERO_TOTAL_MOVIES);
    }

    return topMoviesByScore([
      ...(homeData?.trending || []),
      ...(homeData?.recommended || []),
      ...(homeData?.hindi || []),
      ...genreRows.comedy,
      ...genreRows.anime,
      ...genreRows.crimeThriller,
      ...genreRows.actionHorrorMystery,
      ...preferenceMovies,
    ], HERO_TOTAL_MOVIES);
  }, [heroGenreMovies, homeData, genreRows, preferenceMovies]);

  const heroMovie = heroMovies[0] || null;

  useEffect(() => {
    const sourceMovies = combineMovieLists(
      homeData?.trending || [],
      homeData?.hindi || [],
      genreRows.comedy || [],
      genreRows.anime || [],
    ).slice(0, 6);

    if (!sourceMovies.length) return undefined;

    const warm = () => {
      for (const movie of sourceMovies) {
        warmRowImage(movie);
      }
    };

    if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
      const idleId = window.requestIdleCallback(warm, { timeout: 1800 });
      return () => window.cancelIdleCallback(idleId);
    }

    const timeoutId = window.setTimeout(warm, 500);
    return () => window.clearTimeout(timeoutId);
  }, [homeData?.trending, homeData?.hindi, genreRows.comedy, genreRows.anime, warmRowImage]);

  const showSearchOverlay = searchLoading || searchResults !== null;

  return (
    <div className="app">
      <Navbar
        selectedUser={selectedUser}
        onSearchResults={handleSearchResults}
        currentView={isProfileView ? 'profile' : 'home'}
        onNavigate={handleNavigate}
        onOpenAuth={() => setShowAuthModal(true)}
        onLogout={handleLogout}
        onPersonalize={handlePersonalize}
        isAuthenticated={isAuthenticated}
      />

      {showAuthModal && (
        <Suspense fallback={<ChunkFallback compact />}>
          <AuthModal
            open={showAuthModal}
            onClose={() => setShowAuthModal(false)}
            onSubmit={handleAuthSubmit}
            loading={authLoading}
          />
        </Suspense>
      )}

      {showPreferenceModal && (
        <Suspense fallback={<ChunkFallback compact />}>
          <ColdStartModal
            open={showPreferenceModal}
            onClose={() => setShowPreferenceModal(false)}
            onSubmit={handlePreferenceSubmit}
            onSkip={() => setShowPreferenceModal(false)}
            initialGenres={preferredGenres}
            initialLanguages={preferredLanguages}
            title="Your Preference"
            subtitle="Pick genre and language combinations (for example: Japanese + Action)."
            submitLabel="Update Preference Picks"
            skipLabel="Close"
          />
        </Suspense>
      )}

      {personalizeNotice && (
        <div className="personalize-notice" id="personalize-notice">
          {personalizeNotice}
        </div>
      )}

      {/* Search Results Overlay */}
      {showSearchOverlay && (
        <div className="search-results" id="search-results">
          <h2 className="search-results__title">
            Results for <span>"{searchQuery || '...' }"</span>
          </h2>
          {searchLoading ? (
            <div className="search-results__skeleton-grid">
              {Array.from({ length: 12 }).map((_, index) => (
                <div
                  key={`search-skeleton-${index}`}
                  className="skeleton search-results__skeleton-card"
                />
              ))}
            </div>
          ) : (searchResults && searchResults.length > 0) ? (
            <div className="search-results__grid">
              {searchResults.map((item, index) => {
                const movie = item.movie || item;
                if (!movie || typeof movie !== 'object' || movie.movie_id == null) {
                  return null;
                }
                return (
                  <MovieCard
                    key={movie.movie_id}
                    movie={movie}
                    onClick={handleMovieClick}
                    watchlist={watchlist}
                    onToggleWatchlist={handleToggleWatchlist}
                    watched={watched}
                    onToggleWatched={handleToggleWatched}
                    userId={authUserId}
                    imageLoading={index < 6 ? 'eager' : 'lazy'}
                    imageFetchPriority={index < 3 ? 'high' : 'auto'}
                    ultraFastImages
                  />
                );
              })}
            </div>
          ) : searchQuery ? (
            <div className="search-results__empty">
              No movies found for "{searchQuery}"
            </div>
          ) : null}
        </div>
      )}

      {/* Movie Detail Modal */}
      {selectedMovie && (
        <Suspense fallback={<ChunkFallback modal />}>
          <MovieModal
            movie={selectedMovie}
            onClose={() => setSelectedMovie(null)}
            onMovieClick={handleMovieClick}
            userId={authUserId}
            userRatings={userRatings}
            watchlist={watchlist}
            onToggleWatchlist={handleToggleWatchlist}
            watched={watched}
            onToggleWatched={handleToggleWatched}
            onRatingSaved={handleRatingSaved}
            onPreferenceChanged={handlePreferenceChanged}
          />
        </Suspense>
      )}

      {isProfileView ? (
        <Suspense fallback={<ChunkFallback />}>
          <ProfilePage
            profile={profileData}
            loading={profileLoading}
            watchlistMovies={watchlistMovies}
            onBack={() => handleNavigate('home')}
            onMovieClick={handleMovieClick}
            watchlist={watchlist}
            onToggleWatchlist={handleToggleWatchlist}
            watched={watched}
            onToggleWatched={handleToggleWatched}
            userId={authUserId}
          />
        </Suspense>
      ) : (
        <>
          {/* Hero Banner */}
          <Hero
            movie={heroMovie}
            movies={heroMovies}
            onMoreInfo={handleMovieClick}
            onScrollNext={handleScrollToSections}
            userId={authUserId}
          />

          {/* Movie Rows */}
          <div className="page-content" id="home-sections">
            {/* Your Preference */}
            {isAuthenticated && hasPreferenceSelection && (
              <MovieRow
                title="✨ Your Preference"
                subtitle="Picks based on your selected genres and languages"
                movies={preferenceMovies}
                onMovieClick={handleMovieClick}
                loading={preferenceLoading}
                watchlist={watchlist}
                onToggleWatchlist={handleToggleWatchlist}
                watched={watched}
                onToggleWatched={handleToggleWatched}
                userId={authUserId}
                ultraFastImages
              />
            )}

            {/* Trending */}
            <MovieRow
              title="🔥 Trending Now"
              movies={homeData?.trending}
              onMovieClick={handleMovieClick}
              loading={loading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              prioritizeImages
              ultraFastImages
            />

            <MovieRow
              title="Comedy"
              subtitle="Top comedy picks"
              movies={genreRows.comedy}
              onMovieClick={handleMovieClick}
              loading={genreRowsLoading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              prioritizeImages
              ultraFastImages
            />

            <MovieRow
              title="Japanese Anime (Animation)"
              subtitle="Top animation picks focused on Japanese titles"
              movies={genreRows.anime}
              onMovieClick={handleMovieClick}
              loading={genreRowsLoading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              ultraFastImages
            />

            <MovieRow
              title="Crime + Thriller"
              subtitle="Crime stories and edge-of-seat thrillers"
              movies={genreRows.crimeThriller}
              onMovieClick={handleMovieClick}
              loading={genreRowsLoading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              ultraFastImages
            />

            <MovieRow
              title="Action + Horror + Mystery"
              subtitle="High-energy action mixed with horror and mystery"
              movies={genreRows.actionHorrorMystery}
              onMovieClick={handleMovieClick}
              loading={genreRowsLoading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              ultraFastImages
            />

            {/* Recommended for user */}
            {homeData?.recommended && (
              <MovieRow
                title="❤️ Recommended For You"
                subtitle={`Personalized for ${selectedUser?.user_name || 'you'}`}
                movies={homeData.recommended}
                onMovieClick={handleMovieClick}
                watchlist={watchlist}
                onToggleWatchlist={handleToggleWatchlist}
                watched={watched}
                onToggleWatched={handleToggleWatched}
                userId={authUserId}
                ultraFastImages
              />
            )}

            {/* Watchlist */}
            {watchlistMovies.length > 0 && (
              <MovieRow
                title="🔖 My Watchlist"
                subtitle="Saved by you"
                movies={watchlistMovies}
                onMovieClick={handleMovieClick}
                watchlist={watchlist}
                onToggleWatchlist={handleToggleWatchlist}
                watched={watched}
                onToggleWatched={handleToggleWatched}
                userId={authUserId}
                ultraFastImages
              />
            )}

            {/* Hindi Movies */}
            <MovieRow
              title="🇮🇳 Popular Hindi Movies"
              movies={homeData?.hindi}
              onMovieClick={handleMovieClick}
              loading={loading}
              watchlist={watchlist}
              onToggleWatchlist={handleToggleWatchlist}
              watched={watched}
              onToggleWatched={handleToggleWatched}
              userId={authUserId}
              ultraFastImages
            />

            {/* Because you watched X */}
            {homeData?.explained?.map((group, i) => (
              <MovieRow
                key={`explained-${i}`}
                title={`Because you watched ${group.source_title}`}
                movies={group.recommendations}
                onMovieClick={handleMovieClick}
                watchlist={watchlist}
                onToggleWatchlist={handleToggleWatchlist}
                watched={watched}
                onToggleWatched={handleToggleWatched}
                userId={authUserId}
                ultraFastImages
              />
            ))}
          </div>
        </>
      )}

      {/* Footer */}
      <footer className="footer">
        <div className="footer__content">
          <div className="footer__brand">CineMatch</div>
          <p>
            A hybrid movie recommendation system combining collaborative filtering,
            content-based NLP, and popularity ranking. Supports Hindi and global content.
            Built with Django, React, and implicit ALS.
          </p>
          <p className="footer__meta">
            Data: MovieLens 20M + TMDB • Models: TF-IDF + ALS + Hybrid Scoring
          </p>
        </div>
      </footer>
    </div>
  );
}

