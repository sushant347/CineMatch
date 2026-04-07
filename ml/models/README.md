# Model Artifacts

Serialized model artifacts (for example .pkl files) are treated as local build outputs and are ignored by git.

To regenerate locally:

```powershell
backend/.venv/Scripts/python.exe ml/train_models.py
```

Keep source code in git; regenerate model binaries per environment when needed.
