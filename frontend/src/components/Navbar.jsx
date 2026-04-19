import { useState, useEffect, useRef } from 'react';
import { searchMovies } from '../api/client';

export default function Navbar({
  selectedUser,
  onSearchResults,
  currentView,
  onNavigate,
  onOpenAuth,
  onLogout,
  onPersonalize,
  isAuthenticated,
}) {
  const [scrolled, setScrolled] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const searchContainerRef = useRef(null);
  const searchRef = useRef(null);
  const debounceRef = useRef(null);
  const abortRef = useRef(null);
  const requestCounterRef = useRef(0);
  const searchCacheRef = useRef(new Map());

  const clearSearch = () => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
      debounceRef.current = null;
    }
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setSearchQuery('');
    onSearchResults(null, '', false);
  };

  useEffect(() => {
    const handleScroll = () => setScrolled(window.scrollY > 50);
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  useEffect(() => {
    if (searchOpen && searchRef.current) {
      searchRef.current.focus();
    }
  }, [searchOpen]);

  useEffect(() => {
    const handleOutsideClick = (event) => {
      const clickedInsideSearchResults = event.target?.closest?.('#search-results');
      const clickedInsideMovieModal = event.target?.closest?.('#movie-modal-overlay')
        || event.target?.closest?.('#movie-modal');
      if (clickedInsideSearchResults) {
        return;
      }
      if (clickedInsideMovieModal) {
        return;
      }

      if (
        searchOpen
        && searchContainerRef.current
        && !searchContainerRef.current.contains(event.target)
      ) {
        setSearchOpen(false);
        clearSearch();
      }
    };

    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, [searchOpen, onSearchResults]);

  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  const handleSearchChange = (e) => {
    const q = e.target.value;
    setSearchQuery(q);

    if (debounceRef.current) clearTimeout(debounceRef.current);

    const normalized = q.trim();
    if (normalized.length < 2) {
      if (abortRef.current) {
        abortRef.current.abort();
        abortRef.current = null;
      }
      onSearchResults(null, '', false);
      return;
    }

    const cacheKey = normalized.toLowerCase();
    if (searchCacheRef.current.has(cacheKey)) {
      onSearchResults(searchCacheRef.current.get(cacheKey), normalized, false);
      return;
    }

    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }

      const controller = new AbortController();
      abortRef.current = controller;
      const requestId = requestCounterRef.current + 1;
      requestCounterRef.current = requestId;

      onSearchResults([], normalized, true);

      try {
        const results = await searchMovies(normalized, 20, { signal: controller.signal });
        if (requestId !== requestCounterRef.current) return;

        searchCacheRef.current.set(cacheKey, results);
        if (searchCacheRef.current.size > 30) {
          const oldestKey = searchCacheRef.current.keys().next().value;
          searchCacheRef.current.delete(oldestKey);
        }

        onSearchResults(results, normalized, false);
      } catch (err) {
        if (err?.name === 'AbortError') return;
        console.error('Search error:', err);
        if (requestId === requestCounterRef.current) {
          onSearchResults([], normalized, false);
        }
      }
    }, 220);
  };

  const handleSearchToggle = () => {
    if (searchOpen) {
      clearSearch();
    }
    setSearchOpen(!searchOpen);
  };

  return (
    <nav className={`navbar ${scrolled ? 'scrolled' : ''}`}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '32px' }}>
        <div className="navbar__logo">CineMatch</div>
        <div className="navbar__nav-links">
          {currentView === 'profile' ? (
            <button
              className="navbar__view-link"
              onClick={() => onNavigate('home')}
              id="nav-home-btn"
            >
              ← Home
            </button>
          ) : (
            <button
              className="navbar__view-link"
              onClick={() => onNavigate('profile')}
              disabled={!selectedUser}
              id="nav-profile-btn"
            >
              My Taste
            </button>
          )}
        </div>
      </div>

      <div className={`navbar__right ${searchOpen ? 'navbar__right--search-open' : ''}`}>
        <button
          className={`navbar__icon-btn navbar__personalize-btn ${searchOpen ? 'navbar__personalize-btn--shifted' : ''}`}
          onClick={onPersonalize}
          aria-label="Get personalized recommendations"
          id="nav-personalize-btn"
          title="Get personalized recommendations"
        >
          ✨
        </button>

        <div className={`search-container ${searchOpen ? 'open' : ''}`} ref={searchContainerRef}>
          <div className={`search-input-wrapper ${searchOpen ? 'active' : ''}`}>
            <span className="search-icon">🔍</span>
            <input
              ref={searchRef}
              type="text"
              placeholder="Search movies..."
              value={searchQuery}
              onChange={handleSearchChange}
              id="search-input"
            />
          </div>
          <button
            className="search-toggle"
            onClick={handleSearchToggle}
            aria-label="Toggle search"
            id="search-toggle-btn"
          >
            {searchOpen ? '✕' : '🔍'}
          </button>
        </div>

        {isAuthenticated && (
          <div className="navbar__auth-chip" title={`Signed in as ${selectedUser?.user_name || 'user'}`}>
            Signed in: {selectedUser?.user_name}
          </div>
        )}

        {isAuthenticated ? (
          <button className="navbar__login-btn" onClick={onLogout} id="nav-logout-btn">
            Logout
          </button>
        ) : (
          <button className="navbar__login-btn" onClick={onOpenAuth} id="nav-login-btn">
            Login / Sign up
          </button>
        )}
      </div>
    </nav>
  );
}

