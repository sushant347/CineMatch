const API_BASE = '/api';

class ApiError extends Error {
  constructor(message, status, payload) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.payload = payload;
  }
}

let csrfToken = null;
let csrfTokenRequest = null;
const GET_CACHE_TTL_MS = 30000;
const getCacheStore = new Map();
const getInflightStore = new Map();

function isUnsafeMethod(method) {
  return !['GET', 'HEAD', 'OPTIONS', 'TRACE'].includes(method);
}

async function parseJSONSafe(response) {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function firstMessage(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) {
    for (const item of value) {
      const nested = firstMessage(item);
      if (nested) return nested;
    }
    return '';
  }
  if (typeof value === 'object') {
    for (const key of Object.keys(value)) {
      const nested = firstMessage(value[key]);
      if (nested) return nested;
    }
  }
  return '';
}

function isCsrfErrorPayload(payload) {
  const rawError = typeof payload?.error === 'string' ? payload.error.trim() : '';
  const rawDetail = typeof payload?.detail === 'string' ? payload.detail.trim() : '';
  const rawCombined = `${rawError} ${rawDetail}`.toLowerCase();
  return rawCombined.includes('csrf');
}

function toErrorMessage(payload, status) {
  const rawError = typeof payload?.error === 'string' ? payload.error.trim() : '';
  const rawDetail = typeof payload?.detail === 'string' ? payload.detail.trim() : '';
  const rawCombined = `${rawError} ${rawDetail}`.toLowerCase();

  if (rawCombined.includes('csrf')) {
    return 'Your security token expired. Please try again.';
  }

  if (status === 401) {
    if (rawCombined.includes('authentication required') || rawCombined.includes('not authenticated')) {
      return 'Your session expired. Please log in again.';
    }
    return rawError || rawDetail || 'Invalid username or password. Please try again.';
  }

  if (status === 403) {
    return rawError || rawDetail || 'You are not allowed to perform this action.';
  }

  if (status === 429) {
    return rawError || rawDetail || 'Too many attempts. Please wait a moment and try again.';
  }

  if (status >= 500) {
    return rawError || rawDetail || 'Something went wrong on the server. Please try again soon.';
  }

  if (!payload) return 'Unable to complete the request right now.';
  if (rawError) return rawError;
  if (rawDetail) return rawDetail;

  const fallback = firstMessage(payload);
  if (fallback) return fallback;
  return 'Unable to complete the request right now.';
}

function cacheCsrfToken(payload) {
  if (payload && typeof payload.csrfToken === 'string' && payload.csrfToken.length > 0) {
    csrfToken = payload.csrfToken;
  }
}

function getCacheKey(url) {
  return `GET:${url}`;
}

function readGetCache(cacheKey) {
  const cached = getCacheStore.get(cacheKey);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    getCacheStore.delete(cacheKey);
    return null;
  }
  return cached.payload;
}

function writeGetCache(cacheKey, payload) {
  getCacheStore.set(cacheKey, {
    payload,
    expiresAt: Date.now() + GET_CACHE_TTL_MS,
  });
}

function clearGetCache() {
  getCacheStore.clear();
  getInflightStore.clear();
}

function invalidateGetCache(prefixes = []) {
  if (!Array.isArray(prefixes) || prefixes.length === 0) {
    clearGetCache();
    return;
  }

  for (const key of getCacheStore.keys()) {
    if (prefixes.some((prefix) => key.includes(prefix))) {
      getCacheStore.delete(key);
    }
  }
}

export async function ensureCsrfToken(force = false) {
  if (csrfToken && !force) return csrfToken;
  if (csrfTokenRequest && !force) return csrfTokenRequest;

  csrfTokenRequest = fetch(`${API_BASE}/auth/csrf/`, {
    method: 'GET',
    credentials: 'include',
  })
    .then(async (response) => {
      const payload = await parseJSONSafe(response);
      cacheCsrfToken(payload);

      if (!response.ok) {
        throw new ApiError(toErrorMessage(payload, response.status), response.status, payload);
      }

      return csrfToken;
    })
    .finally(() => {
      csrfTokenRequest = null;
    });

  return csrfTokenRequest;
}

export async function initializeAuthSession() {
  try {
    await ensureCsrfToken();
  } catch {
    // Do not block app rendering if CSRF bootstrap fails temporarily.
  }
}

async function fetchJSON(url, options = {}) {
  const method = (options.method || 'GET').toUpperCase();
  const headers = new Headers(options.headers || {});
  const cacheKey = method === 'GET' ? getCacheKey(url) : null;
  const requestOptions = {
    ...options,
    method,
    headers,
    credentials: 'include',
  };

  if (
    requestOptions.body !== undefined
    && !(requestOptions.body instanceof FormData)
    && !headers.has('Content-Type')
  ) {
    headers.set('Content-Type', 'application/json');
  }

  if (isUnsafeMethod(method)) {
    await ensureCsrfToken();
    if (csrfToken && !headers.has('X-CSRFToken')) {
      headers.set('X-CSRFToken', csrfToken);
    }
  }

  if (cacheKey) {
    const cachedPayload = readGetCache(cacheKey);
    if (cachedPayload !== null) {
      return cachedPayload;
    }

    if (getInflightStore.has(cacheKey)) {
      return getInflightStore.get(cacheKey);
    }
  }

  const executeRequest = async () => {
    const send = async () => {
      const response = await fetch(url, requestOptions);
      const payload = await parseJSONSafe(response);
      cacheCsrfToken(payload);
      return { response, payload };
    };

    let { response, payload } = await send();

    // CSRF tokens can become stale after session rotation; refresh once and retry.
    if (
      isUnsafeMethod(method)
      && response.status === 403
      && isCsrfErrorPayload(payload)
    ) {
      await ensureCsrfToken(true);
      if (csrfToken) {
        headers.set('X-CSRFToken', csrfToken);
        ({ response, payload } = await send());
      }
    }

    if (!response.ok) {
      throw new ApiError(toErrorMessage(payload, response.status), response.status, payload);
    }

    return payload;
  };

  if (cacheKey) {
    const inflight = executeRequest()
      .then((payload) => {
        writeGetCache(cacheKey, payload);
        return payload;
      })
      .finally(() => {
        getInflightStore.delete(cacheKey);
      });

    getInflightStore.set(cacheKey, inflight);
    return inflight;
  }

  return executeRequest();
}

export async function getHomeData(userId) {
  const qs = userId ? `?user_id=${userId}` : '';
  return fetchJSON(`${API_BASE}/home/${qs}`);
}

export async function getTrending(n = 20) {
  return fetchJSON(`${API_BASE}/trending/?n=${n}`);
}

export async function getHindiMovies(n = 20) {
  return fetchJSON(`${API_BASE}/hindi/?n=${n}`);
}

export async function getHeroMovies(n = 8) {
  return fetchJSON(`${API_BASE}/hero/?n=${n}`);
}

export async function getGenreMovies(genre, n = 20) {
  return fetchJSON(`${API_BASE}/genre/${encodeURIComponent(genre)}/?n=${n}`);
}

export async function getLanguageMovies(language, n = 20) {
  return fetchJSON(`${API_BASE}/language/${encodeURIComponent(language)}/?n=${n}`);
}

export async function getUserRecommendations(userId, n = 20) {
  return fetchJSON(`${API_BASE}/recommend/user/${userId}/?n=${n}`);
}

export async function getSimilarMovies(movieId, n = 10) {
  return fetchJSON(`${API_BASE}/recommend/movie/${movieId}/?n=${n}`);
}

export async function getMovieDetail(movieId) {
  return fetchJSON(`${API_BASE}/movies/${movieId}/`);
}

export async function searchMovies(query, n = 20, options = {}) {
  return fetchJSON(`${API_BASE}/search/?q=${encodeURIComponent(query)}&n=${n}`, {
    signal: options.signal,
  });
}

export async function getColdStartRecs(genres, languages) {
  return fetchJSON(`${API_BASE}/coldstart/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ genres, languages }),
  });
}

export async function getUsers() {
  return fetchJSON(`${API_BASE}/users/`);
}

export async function registerUser(username, password, confirmPassword) {
  const payload = await fetchJSON(`${API_BASE}/auth/register/`, {
    method: 'POST',
    body: JSON.stringify({ username, password, confirm_password: confirmPassword }),
  });
  clearGetCache();
  cacheCsrfToken(payload);
  return payload;
}

export async function loginUser(username, password) {
  const payload = await fetchJSON(`${API_BASE}/auth/login/`, {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
  clearGetCache();
  cacheCsrfToken(payload);
  return payload;
}

export async function logoutUser() {
  const payload = await fetchJSON(`${API_BASE}/auth/logout/`, {
    method: 'POST',
  });
  clearGetCache();
  await ensureCsrfToken(true).catch(() => {});
  return payload;
}

export async function getAuthMe() {
  return fetchJSON(`${API_BASE}/auth/me/`);
}

export async function getAuthPreferences() {
  return fetchJSON(`${API_BASE}/auth/preferences/`);
}

export async function saveAuthPreferences(genres, languages) {
  const payload = await fetchJSON(`${API_BASE}/auth/preferences/`, {
    method: 'POST',
    body: JSON.stringify({ genres, languages }),
  });
  invalidateGetCache(['/auth/preferences/', '/coldstart/', '/home/']);
  return payload;
}

export async function getTopRated() {
  return fetchJSON(`${API_BASE}/top-rated/`);
}

export async function getExplainedRecs(userId) {
  return fetchJSON(`${API_BASE}/explain/${userId}/`);
}

export async function rateMovie(_userId, movieId, rating) {
  const payload = await fetchJSON(`${API_BASE}/rate/`, {
    method: 'POST',
    body: JSON.stringify({ movie_id: movieId, rating }),
  });
  invalidateGetCache(['/ratings/', '/home/', '/profile/', '/recommend/', '/explain/']);
  return payload;
}

export async function getUserRatings(userId) {
  return fetchJSON(`${API_BASE}/ratings/${userId}/`);
}

export async function toggleWatchlist(_userId, movieId) {
  const payload = await fetchJSON(`${API_BASE}/watchlist/toggle/`, {
    method: 'POST',
    body: JSON.stringify({ movie_id: movieId }),
  });
  invalidateGetCache(['/watchlist/', '/profile/', '/home/']);
  return payload;
}

export async function getWatchlist(userId) {
  return fetchJSON(`${API_BASE}/watchlist/${userId}/`);
}

export async function toggleWatched(_userId, movieId) {
  const payload = await fetchJSON(`${API_BASE}/watched/toggle/`, {
    method: 'POST',
    body: JSON.stringify({ movie_id: movieId }),
  });
  invalidateGetCache(['/watched/', '/home/', '/profile/', '/recommend/', '/explain/']);
  return payload;
}

export async function getWatchedMovies(userId) {
  return fetchJSON(`${API_BASE}/watched/${userId}/`);
}

export async function getMovieReviews(movieId, _userId) {
  return fetchJSON(`${API_BASE}/reviews/${movieId}/`);
}

export async function writeMovieReview(_userId, movieId, reviewText) {
  const payload = await fetchJSON(`${API_BASE}/reviews/write/`, {
    method: 'POST',
    body: JSON.stringify({
      movie_id: movieId,
      review_text: reviewText,
    }),
  });
  invalidateGetCache([`/reviews/${movieId}/`, '/profile/']);
  return payload;
}

export async function getUserProfile(userId) {
  return fetchJSON(`${API_BASE}/profile/${userId}/`);
}
