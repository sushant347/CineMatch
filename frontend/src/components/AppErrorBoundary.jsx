import React from 'react';

export default class AppErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error, errorInfo) {
    // Keep console logging for debugging in development.
    console.error('AppErrorBoundary caught an error:', error, errorInfo);
  }

  handleReload = () => {
    window.location.reload();
  };

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          minHeight: '100vh',
          display: 'grid',
          placeItems: 'center',
          background: '#141414',
          color: '#fff',
          padding: '24px',
          textAlign: 'center',
          fontFamily: 'Inter, sans-serif',
        }}>
          <div>
            <h1 style={{ marginBottom: '12px' }}>Something went wrong</h1>
            <p style={{ marginBottom: '18px', color: '#b3b3b3' }}>
              The app hit a runtime error. Reload to recover.
            </p>
            <button
              type="button"
              onClick={this.handleReload}
              style={{
                background: '#e50914',
                color: '#fff',
                border: 'none',
                borderRadius: '8px',
                padding: '10px 18px',
                cursor: 'pointer',
              }}
            >
              Reload
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

