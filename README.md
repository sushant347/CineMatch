# CineMatch

Hybrid Netflix-style movie recommendation system with a React frontend, Django REST backend, and a hybrid ML engine.

## Tech Stack

- Frontend: React, Vite, vanilla CSS
- Backend: Django, Django REST Framework
- ML: scikit-learn, pandas, numpy, scipy
- Database: SQLite (dev) / PostgreSQL (optional)

## Project Structure

```text
netflix/
├── backend/
│   ├── config/
│   ├── recommender/
│   └── manage.py
├── frontend/
│   ├── src/
│   └── package.json
├── data/
│   ├── prepare_data.py
│   └── processed/
│       ├── movies_merged.csv
│       └── ratings_subset.csv
├── ml/
│   ├── train_models.py
│   ├── evaluate.py
│   └── models/
├── requirements.txt
└── README.md
```

## API Overview

- `GET /api/home/?user_id=<id>`
- `GET /api/recommend/user/<id>/`
- `GET /api/recommend/movie/<id>/`
- `GET /api/trending/`
- `GET /api/hindi/`
- `GET /api/top-rated/`
- `GET /api/search/?q=<query>`
- `GET /api/movies/<id>/`
- `POST /api/coldstart/`
- `POST /api/auth/register/`, `POST /api/auth/login/`, `POST /api/auth/logout/`

## Local Setup

### 1) Python environment

```powershell
py -3.12 -m venv backend/.venv
backend/.venv/Scripts/python.exe -m pip install --upgrade pip
backend/.venv/Scripts/python.exe -m pip install -r requirements.txt
```

Optional training dependencies:

```powershell
backend/.venv/Scripts/python.exe -m pip install -r ml/requirements-ml.txt
```

### 2) Backend config

Copy `backend/.env.example` to `backend/.env` and adjust values if needed.

### 3) Data and models

This repository keeps core processed datasets in git:

- `data/processed/movies_merged.csv`
- `data/processed/ratings_subset.csv`

Raw datasets and large generated artifacts remain ignored by default.

- Data prep: `backend/.venv/Scripts/python.exe data/prepare_data.py`
- Model training: `backend/.venv/Scripts/python.exe ml/train_models.py`
- Evaluation: `backend/.venv/Scripts/python.exe ml/evaluate.py`

### 4) Run backend

```powershell
Set-Location backend
./.venv/Scripts/python.exe manage.py migrate
./.venv/Scripts/python.exe manage.py runserver 8000
```

### 5) Run frontend

```powershell
Set-Location frontend
npm install
npm run dev
```

Frontend runs on `http://localhost:3000` and proxies `/api` to `http://localhost:8000`.

## GitHub Readiness Notes

- Ignored local artifacts:
  - virtual environments
  - node modules and frontend build output
  - SQLite DB file
  - raw data files under `data/raw`
  - generated processed outputs except `data/processed/movies_merged.csv` and `data/processed/ratings_subset.csv`
  - local model binaries under `ml/models/*.pkl`
- Keep source code, scripts, migrations, and docs in git.

## License

All copyright (c) 2024 CineMatch. All rights reserved. Unauthorized use, reproduction, or distribution is prohibited. For licensing inquiries, contact sushant98677@gmail.com