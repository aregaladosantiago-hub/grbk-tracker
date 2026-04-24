import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd

SNAPSHOT_COLUMNS = [
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "community",
    "address", "lot", "plan", "status", "price", "prior_price", "sqft", "beds", "baths",
    "garage", "incentive_text", "raw_text", "qa_flag"
]


def load_snapshots(snapshot_dir: str) -> pd.DataFrame:
    files = sorted(Path(snapshot_dir).glob("*.csv"))
    frames: List[pd.DataFrame] = []
    for file in files:
        df = pd.read_csv(file)
        if "snapshot_date" not in df.columns:
            df["snapshot_date"] = file.stem
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    all_df = pd.concat(frames, ignore_index=True)
    all_df["snapshot_date"] = pd.to_datetime(all_df["snapshot_date"], errors="coerce")
    return all_df


def pct_change(current, prior):
    if prior in [0, None] or pd.isna(prior):
        return None
    return (current - prior) / prior


def generate_report(snapshot_dir: str, config_path: str, out_path: str):
    with open(config_path, "r") as f:
        cfg = json.load(f)
    removal_days = int(cfg.get("settings", {}).get("removal_watch_days", 7))
    high_conf_days = int(cfg.get("settings", {}).get("high_confidence_removal_days", 14))

    df = load_snapshots(snapshot_dir)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if df.empty or df["snapshot_date"].isna().all():
        Path(out_path).write_text("# GRBK Weekly Inventory Tracker\n\nNo snapshot data yet. Run the scraper first.\n")
        return

    latest_date = df["snapshot_date"].max()
    prior_week_date = latest_date - pd.Timedelta(days=7)
    week_start = latest_date - pd.Timedelta(days=7)

    latest = df[df["snapshot_date"] == latest_date].copy()
    prior_dates = df[df["snapshot_date"] <= prior_week_date]["snapshot_date"]
    prior_date = prior_dates.max() if not prior_dates.empty else None
    prior = df[df["snapshot_date"] == prior_date].copy() if prior_date is not None else pd.DataFrame(columns=df.columns)

    latest_keys = set(zip(latest["brand"], latest["home_key"]))
    prior_keys = set(zip(prior["brand"], prior["home_key"])) if not prior.empty else set()
    new_keys = latest_keys - prior_keys
    removed_keys = prior_keys - latest_keys

    latest["has_price_cut"] = (
        latest["prior_price"].notna() & latest["price"].notna() & (latest["prior_price"] > latest["price"])
    )
    latest["price_cut_amount"] = latest["prior_price"] - latest["price"]
    latest["has_incentive"] = latest["incentive_text"].notna() & (latest["incentive_text"].astype(str).str.len() > 0)

    brand_summary = latest.groupby("brand").agg(
        active_listings=("home_key", "count"),
        price_cuts=("has_price_cut", "sum"),
        incentives=("has_incentive", "sum"),
        median_price=("price", "median"),
    ).reset_index().sort_values("active_listings", ascending=False)

    # First/last seen across all snapshots.
    history = df.groupby(["brand", "home_key"]).agg(
        first_seen=("snapshot_date", "min"),
        last_seen=("snapshot_date", "max"),
        observations=("snapshot_date", "nunique"),
    ).reset_index()
    active_history = history[history.apply(lambda r: (r["brand"], r["home_key"]) in latest_keys, axis=1)].copy()
    active_history["days_seen"] = (latest_date - active_history["first_seen"]).dt.days + 1

    removed_history = history[history["last_seen"] < latest_date].copy()
    removed_history["days_absent"] = (latest_date - removed_history["last_seen"]).dt.days
    possible_removed = removed_history[removed_history["days_absent"] >= 1]
    probable_removed = removed_history[removed_history["days_absent"] >= removal_days]
    high_conf_removed = removed_history[removed_history["days_absent"] >= high_conf_days]

    # Changes CSVs for manual QA.
    changes_dir = Path("reports")
    latest.to_csv(changes_dir / "latest_active.csv", index=False)
    pd.DataFrame(list(new_keys), columns=["brand", "home_key"]).to_csv(changes_dir / "new_vs_prior_week.csv", index=False)
    pd.DataFrame(list(removed_keys), columns=["brand", "home_key"]).to_csv(changes_dir / "removed_vs_prior_week.csv", index=False)
    probable_removed.to_csv(changes_dir / "probable_removed.csv", index=False)

    lines = []
    lines.append("# GRBK Weekly Inventory & Absorption Tracker")
    lines.append("")
    lines.append(f"Latest snapshot: **{latest_date.date()}**")
    lines.append("")
    lines.append("## Executive read")
    lines.append("")
    lines.append("This tracker measures public website listing behavior, not official GRBK orders, closings, or backlog. Treat removed listings as a sell-through proxy only after manual QA.")
    lines.append("")
    lines.append("## Top-level metrics")
    lines.append("")
    top = pd.DataFrame([
        ["Active listings", len(latest)],
        ["New listings vs prior week snapshot", len(new_keys)],
        ["Removed listings vs prior week snapshot", len(removed_keys)],
        [f"Possible removals, absent 1+ day", len(possible_removed)],
        [f"Probable removals, absent {removal_days}+ days", len(probable_removed)],
        [f"High-confidence removals, absent {high_conf_days}+ days", len(high_conf_removed)],
        ["Listings with price cuts", int(latest["has_price_cut"].sum()) if not latest.empty else 0],
        ["Listings with incentive language", int(latest["has_incentive"].sum()) if not latest.empty else 0],
        ["Median active listing price", round(latest["price"].median(), 0) if latest["price"].notna().any() else None],
        ["Median days seen, active listings", round(active_history["days_seen"].median(), 1) if not active_history.empty else None],
    ], columns=["Metric", "Value"])
    lines.append(top.to_markdown(index=False))
    lines.append("")
    lines.append("## Active inventory by brand")
    lines.append("")
    lines.append(brand_summary.to_markdown(index=False) if not brand_summary.empty else "No active listings captured.")
    lines.append("")
    lines.append("## Interpretation framework")
    lines.append("")
    lines.append("Positive signal: inventory flat/down, removals rising, limited price cuts, limited incentives, ready-now inventory not aging.")
    lines.append("")
    lines.append("Negative signal: inventory building, removals slowing, broader price cuts, more incentives, completed inventory aging.")
    lines.append("")
    lines.append("## QA caveats")
    lines.append("")
    lines.append("- A removed listing is not automatically a sale. It may be under contract, relisted, temporarily hidden, or moved to a new URL.")
    lines.append("- The first few weeks are for parser validation; do not use the data in a write-up until the captured listings match the websites manually.")
    lines.append("- Brand-specific parsers will likely be needed for cleaner address, community, lot, and plan extraction.")
    lines.append("")
    Path(out_path).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate GRBK weekly tracker report from daily snapshots.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    parser.add_argument("--config", default="config/brands.json")
    parser.add_argument("--out", default="reports/weekly_report.md")
    args = parser.parse_args()
    generate_report(args.snapshot_dir, args.config, args.out)
    print(f"Wrote {args.out}")
