import { useEffect, useState } from 'react';

export default function AuthModal({ open, onClose, onSubmit, loading }) {
  const [mode, setMode] = useState('login');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    if (!open) {
      setMode('login');
      setUsername('');
      setPassword('');
      setConfirmPassword('');
      setError('');
    }
  }, [open]);

  useEffect(() => {
    const onEsc = (event) => {
      if (event.key === 'Escape' && open) {
        onClose();
      }
    };
    document.addEventListener('keydown', onEsc);
    return () => document.removeEventListener('keydown', onEsc);
  }, [open, onClose]);

  if (!open) return null;

  const handleSubmit = async (event) => {
    event.preventDefault();
    setError('');

    const cleanedUsername = username.trim();
    const usernamePattern = /^[A-Za-z0-9_.-]{3,60}$/;
    if (!usernamePattern.test(cleanedUsername)) {
      setError('Username must be 3-60 chars and use only letters, numbers, ., _, or -');
      return;
    }
    if (password.length < 8) {
      setError('Password must be at least 8 characters');
      return;
    }
    if (mode === 'register' && password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    try {
      await onSubmit({
        mode,
        username: cleanedUsername,
        password,
        confirmPassword,
      });
      setMode('login');
      setUsername('');
      setPassword('');
      setConfirmPassword('');
      setError('');
      onClose();
    } catch (err) {
      setError(err?.message || 'Unable to continue right now');
    }
  };

  const isRegister = mode === 'register';

  return (
    <div className="auth-modal-overlay" onClick={onClose} id="auth-modal-overlay">
      <div className="auth-modal" onClick={(e) => e.stopPropagation()} id="auth-modal">
        <button className="auth-modal__close" onClick={onClose} aria-label="Close authentication dialog">
          ✕
        </button>

        <div className="auth-modal__tabs" role="tablist" aria-label="Authentication mode">
          <button
            type="button"
            className={`auth-modal__tab ${!isRegister ? 'active' : ''}`}
            onClick={() => {
              setMode('login');
              setError('');
            }}
          >
            Login
          </button>
          <button
            type="button"
            className={`auth-modal__tab ${isRegister ? 'active' : ''}`}
            onClick={() => {
              setMode('register');
              setError('');
            }}
          >
            Create Account
          </button>
        </div>

        <h2 className="auth-modal__title">
          {isRegister ? 'Create your CineMatch account' : 'Sign in to CineMatch'}
        </h2>
        <p className="auth-modal__subtitle">
          {isRegister
            ? 'Use a unique username and a password with at least 8 characters (avoid common passwords).'
            : 'Sign in to access your personal watchlist, watched history, and recommendations.'}
        </p>

        <form className="auth-modal__form" onSubmit={handleSubmit}>
          <label htmlFor="auth-username">Username</label>
          <input
            id="auth-username"
            type="text"
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="moviefan"
            maxLength={60}
          />

          <label htmlFor="auth-password">Password</label>
          <input
            id="auth-password"
            type="password"
            autoComplete={isRegister ? 'new-password' : 'current-password'}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="••••••"
            maxLength={128}
          />

          {isRegister && (
            <>
              <label htmlFor="auth-confirm-password">Confirm Password</label>
              <input
                id="auth-confirm-password"
                type="password"
                autoComplete="new-password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="••••••"
                maxLength={128}
              />
            </>
          )}

          {error && <p className="auth-modal__error">{error}</p>}

          <button type="submit" className="auth-modal__submit" disabled={loading}>
            {loading
              ? (isRegister ? 'Creating account...' : 'Signing in...')
              : (isRegister ? 'Create Account' : 'Sign In')}
          </button>

          <p className="auth-modal__switch-text">
            {isRegister ? 'Already have an account?' : 'New to CineMatch?'}
            {' '}
            <button
              type="button"
              className="auth-modal__switch-btn"
              onClick={() => {
                setMode(isRegister ? 'login' : 'register');
                setError('');
              }}
            >
              {isRegister ? 'Login' : 'Create one'}
            </button>
          </p>
        </form>
      </div>
    </div>
  );
}

