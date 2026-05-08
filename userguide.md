# User Guide — Horse Racing Betting System

Short cheat sheet for launching things. All commands run from `F:/racing`.

## First-time setup

```
pip install -r requirements.txt
```

Keep NumPy on 1.x — do **not** let pip upgrade it to 2.x.

## Launch the dashboard

```
streamlit run app/app.py --server.port 8501
```

Then open http://localhost:8501. Use the left sidebar to navigate: Dashboard, Data Management, Model Training, Backtesting, Race Predictions, Settings.

## Train a new model

```
python train_model.py
```

Optional flags:
- `--version v1.4` — tag the output directory under `artifacts/models/`
- `--quick` — fast sanity run on a data subset
- `--dry-run` — validate the setup without training

Output lands in `artifacts/models/<version>/` (model, calibrator, metadata, plots).

## Run a backtest

```
python run_backtest.py --model artifacts/models/v1.3 --compare
```

Or a single strategy:
```
python run_backtest.py --model artifacts/models/v1.3 --strategy kelly
```
Strategies: `flat`, `kelly`, `value`, `toppick`, `morning_favorite`, `momentum`.

## Refresh the database from raw XML

```
python run_full_extraction.py
```

Takes ~30–60 minutes to process the `2023 PPs/` and `2023 Result Charts/` folders.

## Where things live

- Dashboard code → `app/`
- Settings → `config/config.yaml`
- Trained models → `artifacts/models/v1.2/`, `v1.3/`
- Database → `racing_data.db`
- Raw data → `2023 PPs/`, `2023 Result Charts/`

## If something breaks

- **Import / LightGBM errors** → check NumPy is on 1.x (`pip show numpy`).
- **Weird distances or race types in the DB** → re-run `python run_full_extraction.py`. Background in `IMPLEMENTATION_REPORT.md`.
- **Dashboard won't start** → confirm port 8501 is free, or pass a different `--server.port`.
- **Need deeper detail** → see `CLAUDE.md`.
