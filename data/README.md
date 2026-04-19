# Data Directory

This project keeps only core processed datasets in git and ignores large raw/generated files.

## Expected Layout

- data/raw/
- data/processed/

Tracked in git:

- data/processed/movies_merged.csv
- data/processed/ratings_subset.csv

Ignored:

- data/raw/*
- other generated artifacts under data/processed/ (for example final_movies.csv)

## How To Populate

1. Place source datasets into data/raw/ (MovieLens, TMDB, etc.).
2. Run:

```powershell
backend/.venv/Scripts/python.exe data/prepare_data.py
```

This generates processed CSV files under data/processed/ for import/training workflows.

