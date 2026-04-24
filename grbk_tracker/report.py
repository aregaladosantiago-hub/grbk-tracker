import argparse
from pathlib import Path

import pandas as pd


def load_snapshots(snapshot_dir: str):
    paths = sorted(Path(snapshot_dir).glob("*.csv"))
    frames = []
    for path in paths:
        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["snapshot_file"] = path.name
        frames.append(df)
    if not frames:
        return pd.DataFrame(), []
    return pd.concat(frames, ignore_index=True), paths


def clean_key(df: pd.DataFrame):
    return (
        df["brand"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + df["community"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + df["address"].fillna("").astype(str).str.lower().str.strip()
    )


def price_cut_mask(df: pd.DataFrame):
    price = pd.to_numeric(df.get("price"), errors="coerce")
    prior = pd.to_numeric(df.get("prior_price"), errors="coerce")
    incentive = df.get("incentive_text", "").fillna("").astype(str).str.lower()
    return ((prior > price) & price.notna()) | incentive.str.contains("price cut|new lower price", regex=True)


def summarize(latest: pd.DataFrame, prior: pd.DataFrame):
    latest = latest.copy()
    prior = prior.copy()

    if not latest.empty:
        latest["listing_key"] = clean_key(latest)
    if not prior.empty:
        prior["listing_key"] = clean_key(prior)

    latest_keys = set(latest["listing_key"]) if not latest.empty else set()
    prior_keys = set(prior["listing_key"]) if not prior.empty else set()

    new_keys = latest_keys - prior_keys if prior_keys else set()
    removed_keys = prior_keys - latest_keys if prior_keys else set()

    active = len(latest_keys)
    new = len(new_keys) if prior_keys else None
    removed = len(removed_keys) if prior_keys else None

    cuts = int(price_cut_mask(latest).sum()) if active else 0
    cut_ratio = cuts / active if active else 0

    return active, new, removed, cuts, cut_ratio, new_keys, removed_keys


def write_csv_outputs(latest: pd.DataFrame, prior: pd.DataFrame, new_keys, removed_keys):
    reports = Path("reports")
    reports.mkdir(exist_ok=True)

    latest.to_csv(reports / "latest_active.csv", index=False)

    if latest.empty:
        pd.DataFrame().to_csv(reports / "new_vs_prior_snapshot.csv", index=False)
    else:
        latest_with_key = latest.copy()
        latest_with_key["listing_key"] = clean_key(latest_with_key)
        latest_with_key[latest_with_key["listing_key"].isin(new_keys)].drop(columns=["listing_key"]).to_csv(
            reports / "new_vs_prior_snapshot.csv", index=False
        )

    if prior.empty:
        pd.DataFrame().to_csv(reports / "removed_vs_prior_snapshot.csv", index=False)
    else:
        prior_with_key = prior.copy()
        prior_with_key["listing_key"] = clean_key(prior_with_key)
        prior_with_key[prior_with_key["listing_key"].isin(removed_keys)].drop(columns=["listing_key"]).to_csv(
            reports / "removed_vs_prior_snapshot.csv", index=False
        )


def generate_report(snapshot_dir: str, out_path: str):
    df, paths = load_snapshots(snapshot_dir)
    if df.empty:
        Path(out_path).write_text("# GRBK Listing Flow Tracker\n\nNo listing rows captured yet.\n")
        return

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    dates = sorted(d for d in df["snapshot_date"].dropna().unique())

    latest_date = dates[-1]
    latest = df[df["snapshot_date"] == latest_date].copy()

    prior = pd.DataFrame()
    prior_date = None
    if len(dates) >= 2:
        prior_date = dates[-2]
        prior = df[df["snapshot_date"] == prior_date].copy()

    active, new, removed, cuts, cut_ratio, new_keys, removed_keys = summarize(latest, prior)
    write_csv_outputs(latest, prior, new_keys, removed_keys)

    new_display = "N/A" if new is None else str(new)
    removed_display = "N/A" if removed is None else str(removed)
    baseline = "No prior snapshot yet" if prior_date is None else str(pd.Timestamp(prior_date).date())

    lines = []
    lines.append("# GRBK Listing Flow Tracker")
    lines.append("")
    lines.append(f"Latest snapshot: **{pd.Timestamp(latest_date).date()}**")
    lines.append(f"Comparison baseline: **{baseline}**")
    lines.append("")
    lines.append("## Core metrics")
    lines.append("")
    lines.append("| Metric | Value | What it means |")
    lines.append("|---|---:|---|")
    lines.append(f"| Active listings | {active} | Current supply visible on tracked brand sites |")
    lines.append(f"| New listings | {new_display} | Homes visible now that were not visible in the prior snapshot |")
    lines.append(f"| Removed listings | {removed_display} | Homes visible in the prior snapshot that are no longer visible; sell-through proxy, not confirmed sales |")
    lines.append(f"| Price-cut ratio | {cut_ratio:.1%} | Active listings showing a lower current price vs prior price or explicit price-cut language |")
    lines.append("")
    lines.append("## How to read it")
    lines.append("")
    lines.append("- **Active up + removals low** = inventory building / slower sell-through.")
    lines.append("- **Active down + removals high** = cleaner demand signal.")
    lines.append("- **High or rising price-cut ratio** = pricing pressure / possible margin risk.")
    lines.append("- **High removals + high price-cut ratio** = homes are moving, but likely with price support.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is a public-listing flow tracker, not official GRBK orders or closings.")
    lines.append("- Removed listings are a sell-through proxy. A home can also disappear because of relisting, URL changes, or temporary website changes.")
    lines.append("- The first snapshot only establishes the baseline; the signal improves after several daily snapshots.")

    Path(out_path).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate simplified GRBK listing flow report.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    parser.add_argument("--out", default="reports/weekly_report.md")
    args = parser.parse_args()
    generate_report(args.snapshot_dir, args.out)
