# GRBK Inventory Tracker

This repository tracks public listing behavior across Green Brick Partners' homebuilder brand websites.

It is designed as an alternative-data research tool. It does **not** directly measure official GRBK orders, closings, or backlog. It captures public website listings and turns changes in those listings into a sell-through and pricing-pressure proxy.

## What it does

Every daily run:

1. Visits the configured GRBK brand websites.
2. Extracts candidate available-home / quick-move-in listings.
3. Saves a dated CSV snapshot in `data/snapshots/`.
4. Writes the latest active snapshot to `reports/latest_active.csv`.
5. Generates `reports/weekly_report.md`.

## Why daily scrape + weekly analysis

The scraper should run daily because listings can appear, disappear, or be relisted within a week. The investment read-through should be weekly because daily website changes are noisy.

## Key outputs

- `data/snapshots/YYYY-MM-DD.csv`: raw daily snapshot
- `reports/latest_active.csv`: latest active listings captured
- `reports/weekly_report.md`: weekly summary
- `reports/new_vs_prior_week.csv`: listings new vs prior week snapshot
- `reports/removed_vs_prior_week.csv`: listings removed vs prior week snapshot
- `reports/probable_removed.csv`: listings absent long enough to qualify as probable removals

## Important caveat

A removed listing is not automatically a sale. It may be under contract, relisted, temporarily hidden, moved to a new URL, or removed for another reason. Treat listing removals as a proxy, and manually QA the data before using it in a write-up.

## Manual run commands

```bash
pip install -r requirements.txt
playwright install chromium
python -m grbk_tracker.scrape --config config/brands.json
python -m grbk_tracker.validate
python -m grbk_tracker.report
```

## GitHub Actions

The workflow in `.github/workflows/daily_scrape.yml` runs this automatically every day and commits the updated CSV/report files back to the repo.

## Build philosophy

Start with broad generic extraction, then improve brand-specific parsers after reviewing the first few snapshots. The first goal is not perfection. The first goal is to prove whether the public websites produce clean enough data to support a useful GRBK research signal.
