import { useEffect, useState } from 'react';

const GENRES = [
  'Action', 'Adventure', 'Animation', 'Comedy', 'Crime',
  'Drama', 'Fantasy', 'Horror', 'Romance', 'Sci-Fi',
  'Thriller', 'Mystery', 'Documentary', 'Family',
];

const LANGUAGES = [
  { code: 'en', label: 'English' },
  { code: 'hi', label: 'Hindi' },
  { code: 'fr', label: 'French' },
  { code: 'es', label: 'Spanish' },
  { code: 'ja', label: 'Japanese' },
  { code: 'ko', label: 'Korean' },
  { code: 'de', label: 'German' },
  { code: 'zh', label: 'Chinese' },
];

export default function ColdStartModal({
  open,
  onSubmit,
  onSkip,
  onClose,
  initialGenres = [],
  initialLanguages = ['en'],
  title = 'Choose Your Preferences',
  subtitle = 'Pick genres and languages you enjoy to tune your recommendations.',
  submitLabel = 'Save Preference Picks',
  skipLabel = 'Maybe later',
}) {
  const [selectedGenres, setSelectedGenres] = useState(initialGenres);
  const [selectedLanguages, setSelectedLanguages] = useState(
    initialLanguages?.length ? initialLanguages : ['en']
  );

  useEffect(() => {
    if (!open) return;
    setSelectedGenres(Array.isArray(initialGenres) ? initialGenres : []);
    setSelectedLanguages(
      Array.isArray(initialLanguages) && initialLanguages.length > 0
        ? initialLanguages
        : ['en']
    );
  }, [open, initialGenres, initialLanguages]);

  if (!open) return null;

  const toggleGenre = (genre) => {
    setSelectedGenres(prev =>
      prev.includes(genre) ? prev.filter(g => g !== genre) : [...prev, genre]
    );
  };

  const toggleLanguage = (code) => {
    setSelectedLanguages(prev =>
      prev.includes(code) ? prev.filter(l => l !== code) : [...prev, code]
    );
  };

  const handleSubmit = () => {
    onSubmit(selectedGenres, selectedLanguages);
  };

  const handleSkip = () => {
    if (onSkip) {
      onSkip();
      return;
    }
    if (onClose) {
      onClose();
    }
  };

  return (
    <div className="coldstart-overlay" id="coldstart-overlay" onClick={handleSkip}>
      <div className="coldstart" id="coldstart-modal" onClick={(event) => event.stopPropagation()}>
        <h2 className="coldstart__title">{title}</h2>
        <p className="coldstart__subtitle">{subtitle}</p>

        <div className="coldstart__section">
          <div className="coldstart__label">Select genres you love</div>
          <div className="coldstart__chips">
            {GENRES.map(genre => (
              <button
                key={genre}
                className={`coldstart__chip ${selectedGenres.includes(genre) ? 'active' : ''}`}
                onClick={() => toggleGenre(genre)}
                id={`genre-chip-${genre.toLowerCase()}`}
              >
                {genre}
              </button>
            ))}
          </div>
        </div>

        <div className="coldstart__section">
          <div className="coldstart__label">Preferred languages</div>
          <div className="coldstart__chips">
            {LANGUAGES.map(lang => (
              <button
                key={lang.code}
                className={`coldstart__chip ${selectedLanguages.includes(lang.code) ? 'active' : ''}`}
                onClick={() => toggleLanguage(lang.code)}
                id={`lang-chip-${lang.code}`}
              >
                {lang.label}
              </button>
            ))}
          </div>
        </div>

        <button className="coldstart__submit" onClick={handleSubmit} id="coldstart-submit-btn">
          {submitLabel}
        </button>

        <button
          className="coldstart__skip"
          onClick={handleSkip}
          id="coldstart-skip-btn"
        >
          {skipLabel}
        </button>
      </div>
    </div>
  );
}
