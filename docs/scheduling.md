# Pipeline Scheduling and Orchestration (GitHub Actions)

This guide outlines how to keep your BigQuery data up-to-date using GitHub Actions to run the DLT pipeline in this repo.

## Overview

- Realtime refresh: run every 5–10 minutes; appends `trip_updates`, `vehicle_positions`, `alerts` into `mta_subway`.
- Static refresh: run once daily (or weekly) to update reference tables (`routes`, `stops`, `trips`, optional `stop_times`, `calendar`).
- Jobs run inside a Python 3.10 environment using your Google service account JSON key.

## Required secrets (GitHub repo → Settings → Secrets and variables → Actions)

- `GCP_PROJECT_ID`: e.g., `push-ai-internal`
- `GCP_SA_KEY_JSON`: contents of your service account JSON (paste as a single-line JSON string)

Optional
- `SOURCES__MTA_GTFS_STATIC__ZIP_URL`: override static GTFS ZIP if needed
- `PIPELINE_FEEDS`: comma-separated feed keys to limit (e.g., `ace,bdfm`)

## Repo structure used

- Entrypoint: `dlt_mta_rtf/load_mta_pipeline.py` with `run(full_static: bool, feeds: Optional[list[str]])`
- Datasets used: `mta_subway` (final), `mta_subway_staging` (staging)

## Recommended schedules

- Realtime: `*/5 * * * *` (every 5 minutes). Adjust if you want lower/higher freshness.
- Static: `0 4 * * *` (daily at 04:00 local). Adjust to off-peak time.

## Example: .github/workflows/pipeline.yml

```yaml
name: MTA GTFS Pipeline

on:
  schedule:
    # Realtime every 5 minutes
    - cron: '*/5 * * * *'
    # Static daily at 04:00 UTC
    - cron: '0 4 * * *'
  workflow_dispatch:

jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    strategy:
      matrix:
        mode: [realtime, static]

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Setup venv and install deps
        run: |
          python -m venv .venv
          source .venv/bin/activate
          pip install --upgrade pip
          pip install -r dlt_mta_rtf/requirements.txt

      - name: Write GCP SA key to file
        run: |
          echo "$GCP_SA_KEY_JSON" > sa.json
        env:
          GCP_SA_KEY_JSON: ${{ secrets.GCP_SA_KEY_JSON }}

      - name: Run pipeline (matrix mode)
        env:
          GOOGLE_APPLICATION_CREDENTIALS: ${{ github.workspace }}/sa.json
          CLOUDSDK_CORE_PROJECT: ${{ secrets.GCP_PROJECT_ID }}
          SOURCES__MTA_GTFS_STATIC__ZIP_URL: ${{ secrets.SOURCES__MTA_GTFS_STATIC__ZIP_URL }}
          PIPELINE_FEEDS: ${{ secrets.PIPELINE_FEEDS }}
        run: |
          source .venv/bin/activate
          FEEDS_ARG=""
          if [ -n "$PIPELINE_FEEDS" ]; then
            # Convert comma list to Python list literal
            FEEDS_ARG="feeds=[\"$(echo $PIPELINE_FEEDS | sed 's/,/\",\"/g')\"]"
          fi

          if [ "${{ matrix.mode }}" = "static" ]; then
            python - <<'PY'
from dlt_mta_rtf import load_mta_pipeline as p
p.run(full_static=True)
PY
          else
            python - <<'PY'
import os
from dlt_mta_rtf import load_mta_pipeline as p
feeds_env = os.environ.get('PIPELINE_FEEDS')
feeds = feeds_env.split(',') if feeds_env else None
p.run(full_static=False, feeds=feeds)
PY
          fi

      - name: Upload logs on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: logs
          path: '**/*.log'
```

Notes
- The matrix runs both jobs on each trigger; the cron entries trigger the workflow twice per hour/day respectively. You can split into two workflows if preferred.
- For very frequent realtime, consider limiting `PIPELINE_FEEDS` (e.g., `ace,bdfm`) to control cost/volume.

## Backfills

- Static: rerun with `full_static=True` for specific dates by pinning `SOURCES__MTA_GTFS_STATIC__ZIP_URL` to a historical snapshot if available.
- Realtime: if you missed intervals, you can simply continue forward; GTFS-RT isn’t a full history, but your `trip_updates`/`vehicle_positions` tables already hold historical `as_of` samples for trend analyses.

## Monitoring and hygiene

- Track failures via Actions notifications.
- Consider creating lightweight views in BigQuery to check counts per hour/day.
- Periodically clean staging (`mta_subway_staging`) if storage grows too large; keep enough history for replay.


