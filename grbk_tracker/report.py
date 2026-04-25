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
    return ((prior > price) & price.notna()) | incentive.str.contains("price cut|new lower price|savings shown", regex=True)


def add_keys(df: pd.DataFrame):
    df = df.copy()
    if not df.empty:
        df["listing_key"] = clean_key(df)
    return df


def metric_row(label, latest, prior):
    latest = add_keys(latest)
    prior = add_keys(prior)

    latest_keys = set(latest["listing_key"]) if not latest.empty else set()
    prior_keys = set(prior["listing_key"]) if not prior.empty else set()

    new_keys = latest_keys - prior_keys if prior_keys else set()
    removed_keys = prior_keys - latest_keys if prior_keys else set()

    active = len(latest_keys)
    new = len(new_keys) if prior_keys else None
    removed = len(removed_keys) if prior_keys else None
    cuts = int(price_cut_mask(latest).sum()) if active else 0
    cut_ratio = cuts / active if active else 0

    return {
        "Segment": label,
        "Active": active,
        "New": "N/A" if new is None else new,
        "Removed": "N/A" if removed is None else removed,
        "Price-cut ratio": f"{cut_ratio:.1%}",
        "Price-cut listings": cuts,
    }, new_keys, removed_keys


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

    total_row, total_new_keys, total_removed_keys = metric_row("Total tracked", latest, prior)

    brand_rows = []
    for brand in sorted(latest["brand"].dropna().unique()):
        brand_latest = latest[latest["brand"] == brand].copy()
        brand_prior = prior[prior["brand"] == brand].copy() if not prior.empty and "brand" in prior.columns else pd.DataFrame()
        row, _, _ = metric_row(brand, brand_latest, brand_prior)
        brand_rows.append(row)

    reports = Path("reports")
    reports.mkdir(exist_ok=True)
    latest.to_csv(reports / "latest_active.csv", index=False)

    latest_with_key = add_keys(latest)
    if total_new_keys:
        latest_with_key[latest_with_key["listing_key"].isin(total_new_keys)].drop(columns=["listing_key"]).to_csv(
            reports / "new_vs_prior_snapshot.csv", index=False
        )
    else:
        pd.DataFrame(columns=latest.columns).to_csv(reports / "new_vs_prior_snapshot.csv", index=False)

    prior_with_key = add_keys(prior)
    if total_removed_keys:
        prior_with_key[prior_with_key["listing_key"].isin(total_removed_keys)].drop(columns=["listing_key"]).to_csv(
            reports / "removed_vs_prior_snapshot.csv", index=False
        )
    else:
        pd.DataFrame(columns=latest.columns).to_csv(reports / "removed_vs_prior_snapshot.csv", index=False)

    baseline = "No prior snapshot yet" if prior_date is None else str(pd.Timestamp(prior_date).date())

    lines = []
    lines.append("# GRBK Listing Flow Tracker")
    lines.append("")
    lines.append(f"Latest snapshot: **{pd.Timestamp(latest_date).date()}**")
    lines.append(f"Comparison baseline: **{baseline}**")
    lines.append("")
    lines.append("## Core metrics")
    lines.append("")
    lines.append("| Segment | Active | New | Removed | Price-cut ratio | Price-cut listings |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    all_rows = [total_row] + brand_rows
    for row in all_rows:
        lines.append(
            f"| {row['Segment']} | {row['Active']} | {row['New']} | {row['Removed']} | "
            f"{row['Price-cut ratio']} | {row['Price-cut listings']} |"
        )

    lines.append("")
    lines.append("## How to read it")
    lines.append("")
    lines.append("- **Active listings** = current supply visible on tracked brand sites.")
    lines.append("- **New listings** = homes visible now that were not visible in the prior snapshot.")
    lines.append("- **Removed listings** = homes visible in the prior snapshot that are no longer visible; sell-through proxy, not confirmed sales.")
    lines.append("- **Price-cut ratio** = active listings showing a lower current price vs prior price or explicit savings/price-cut language.")
    lines.append("")
    lines.append("## Signal framework")
    lines.append("")
    lines.append("- **Active up + removals low** = inventory building / slower sell-through.")
    lines.append("- **Active down + removals high** = cleaner demand signal.")
    lines.append("- **High or rising price-cut ratio** = pricing pressure / possible margin risk.")
    lines.append("- **High removals + high price-cut ratio** = homes are moving, but likely with price support.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is a public-listing flow tracker, not official GRBK orders or closings.")
    lines.append("- Removed listings are a sell-through proxy. A home can disappear because of relisting, URL changes, or temporary website changes.")
    lines.append("- The first snapshot only establishes the baseline; the signal improves after several daily snapshots.")

    Path(out_path).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate simplified GRBK listing flow report.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    parser.add_argument("--out", default="reports/weekly_report.md")
    args = parser.parse_args()
    generate_report(args.snapshot_dir, args.out)
