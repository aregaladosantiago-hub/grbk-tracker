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


def normalized_text(df: pd.DataFrame, column: str):
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[column].fillna("").astype(str).str.lower().str.strip()


def clean_key(df: pd.DataFrame):
    address = normalized_text(df, "address")
    url = normalized_text(df, "url")
    identity = address.where(address.ne(""), url)
    return (
        normalized_text(df, "brand")
        + "|"
        + normalized_text(df, "market")
        + "|"
        + identity
    )


def add_keys(df: pd.DataFrame):
    df = df.copy()
    if not df.empty:
        df["listing_key"] = clean_key(df)
        df["brand_key"] = normalized_text(df, "brand")
    return df


def unique_keys(df: pd.DataFrame):
    if df.empty:
        return set()
    return set(add_keys(df)["listing_key"])


def price_series(df: pd.DataFrame, column: str):
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return pd.to_numeric(df[column], errors="coerce")


def explicit_price_cut_mask(df: pd.DataFrame):
    price = price_series(df, "price")
    prior = price_series(df, "prior_price")
    incentive = normalized_text(df, "incentive_text")
    return ((prior > price) & price.notna()) | incentive.str.contains(
        "price cut|new lower price|savings shown",
        regex=True,
    )


def price_cut_keys(latest: pd.DataFrame, prior: pd.DataFrame):
    latest = add_keys(latest)
    prior = add_keys(prior)
    if latest.empty:
        return set()

    latest_unique = latest.drop_duplicates("listing_key", keep="last").copy()
    cut_keys = set(latest_unique.loc[explicit_price_cut_mask(latest_unique), "listing_key"])

    if prior.empty:
        return cut_keys

    prior_unique = prior.drop_duplicates("listing_key", keep="last").copy()
    compare = latest_unique[["listing_key"]].copy()
    compare["latest_price"] = price_series(latest_unique, "price")

    prior_prices = prior_unique[["listing_key"]].copy()
    prior_prices["prior_snapshot_price"] = price_series(prior_unique, "price")

    merged = compare.merge(prior_prices, on="listing_key", how="inner")
    historical_cut = merged[
        merged["latest_price"].notna()
        & merged["prior_snapshot_price"].notna()
        & (merged["latest_price"] < merged["prior_snapshot_price"])
    ]
    cut_keys.update(historical_cut["listing_key"])
    return cut_keys


def common_brand_pair(latest: pd.DataFrame, prior: pd.DataFrame):
    latest = add_keys(latest)
    prior = add_keys(prior)
    if latest.empty or prior.empty:
        return latest.iloc[0:0], prior.iloc[0:0], set()

    common = set(latest["brand_key"]) & set(prior["brand_key"])
    return (
        latest[latest["brand_key"].isin(common)].copy(),
        prior[prior["brand_key"].isin(common)].copy(),
        common,
    )


def metric_row(label, latest, prior):
    latest = add_keys(latest)
    prior = add_keys(prior)

    active = len(set(latest["listing_key"])) if not latest.empty else 0
    cuts = len(price_cut_keys(latest, prior))
    cut_ratio = cuts / active if active else 0

    comparable_latest, comparable_prior, common = common_brand_pair(latest, prior)
    if prior.empty or not common:
        new = None
        removed = None
        new_keys = set()
        removed_keys = set()
    else:
        latest_keys = set(comparable_latest["listing_key"])
        prior_keys = set(comparable_prior["listing_key"])
        new_keys = latest_keys - prior_keys
        removed_keys = prior_keys - latest_keys
        new = len(new_keys)
        removed = len(removed_keys)

    return {
        "Segment": label,
        "Active": active,
        "Daily new": "N/A" if new is None else new,
        "Daily removed": "N/A" if removed is None else removed,
        "Price-cut ratio": f"{cut_ratio:.1%}",
        "Price-cut listings": cuts,
    }, new_keys, removed_keys


def snapshot_for_date(df: pd.DataFrame, snapshot_date):
    return df[df["snapshot_date"] == snapshot_date].copy()


def flow_keys_between(previous: pd.DataFrame, current: pd.DataFrame, brand: str = None):
    if brand is not None:
        prev = previous[normalized_text(previous, "brand") == brand.lower()].copy()
        curr = current[normalized_text(current, "brand") == brand.lower()].copy()
        if prev.empty or curr.empty:
            return None
    else:
        curr, prev, common = common_brand_pair(current, previous)
        if not common:
            return None

    prev_keys = unique_keys(prev)
    curr_keys = unique_keys(curr)
    return curr_keys - prev_keys, prev_keys - curr_keys


def rolling_flow_row(label, df: pd.DataFrame, dates, latest_date, brand: str = None, days: int = 7):
    latest = snapshot_for_date(df, latest_date)
    if brand is not None:
        latest = latest[normalized_text(latest, "brand") == brand.lower()].copy()

    active = len(unique_keys(latest))
    start_date = pd.Timestamp(latest_date) - pd.Timedelta(days=days)
    window_dates = [d for d in dates if start_date <= pd.Timestamp(d) <= pd.Timestamp(latest_date)]

    added_keys = set()
    removed_keys = set()
    comparable_pairs = 0

    for previous_date, current_date in zip(window_dates, window_dates[1:]):
        previous = snapshot_for_date(df, previous_date)
        current = snapshot_for_date(df, current_date)
        flow = flow_keys_between(previous, current, brand=brand)
        if flow is None:
            continue
        added, removed = flow
        added_keys.update(added)
        removed_keys.update(removed)
        comparable_pairs += 1

    if comparable_pairs == 0:
        added = "N/A"
        removed = "N/A"
    else:
        added = len(added_keys)
        removed = len(removed_keys)

    return {
        "Segment": label,
        "Active": active,
        "Added last 7d": added,
        "Removed proxy last 7d": removed,
        "Comparable daily pairs": comparable_pairs,
    }


def generate_report(snapshot_dir: str, out_path: str):
    df, paths = load_snapshots(snapshot_dir)
    if df.empty:
        Path(out_path).write_text("# GRBK Listing Flow Tracker\n\nNo listing rows captured yet.\n")
        return

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    dates = sorted(d for d in df["snapshot_date"].dropna().unique())

    latest_date = dates[-1]
    latest = snapshot_for_date(df, latest_date)

    prior = pd.DataFrame()
    prior_date = None
    if len(dates) >= 2:
        prior_date = dates[-2]
        prior = snapshot_for_date(df, prior_date)

    total_row, total_new_keys, total_removed_keys = metric_row("Total tracked", latest, prior)

    brand_rows = []
    weekly_rows = [rolling_flow_row("Total tracked", df, dates, latest_date)]
    for brand in sorted(latest["brand"].dropna().unique()):
        brand_latest = latest[latest["brand"] == brand].copy()
        brand_prior = prior[prior["brand"] == brand].copy() if not prior.empty and "brand" in prior.columns else pd.DataFrame()
        row, _, _ = metric_row(brand, brand_latest, brand_prior)
        brand_rows.append(row)
        weekly_rows.append(rolling_flow_row(brand, df, dates, latest_date, brand=brand))

    reports = Path("reports")
    reports.mkdir(exist_ok=True)
    latest.to_csv(reports / "latest_active.csv", index=False)

    latest_with_key = add_keys(latest)
    if total_new_keys:
        latest_with_key[latest_with_key["listing_key"].isin(total_new_keys)].drop(
            columns=["listing_key", "brand_key"]
        ).to_csv(reports / "new_vs_prior_snapshot.csv", index=False)
    else:
        pd.DataFrame(columns=latest.columns).to_csv(reports / "new_vs_prior_snapshot.csv", index=False)

    prior_with_key = add_keys(prior)
    if total_removed_keys:
        prior_with_key[prior_with_key["listing_key"].isin(total_removed_keys)].drop(
            columns=["listing_key", "brand_key"]
        ).to_csv(reports / "removed_vs_prior_snapshot.csv", index=False)
    else:
        pd.DataFrame(columns=latest.columns).to_csv(reports / "removed_vs_prior_snapshot.csv", index=False)

    baseline = "No prior snapshot yet" if prior_date is None else str(pd.Timestamp(prior_date).date())

    lines = []
    lines.append("# GRBK Listing Flow Tracker")
    lines.append("")
    lines.append(f"Latest snapshot: **{pd.Timestamp(latest_date).date()}**")
    lines.append(f"Daily comparison baseline: **{baseline}**")
    lines.append("")
    lines.append("## Current and daily metrics")
    lines.append("")
    lines.append("| Segment | Active | Daily new | Daily removed | Price-cut ratio | Price-cut listings |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in [total_row] + brand_rows:
        lines.append(
            f"| {row['Segment']} | {row['Active']} | {row['Daily new']} | {row['Daily removed']} | "
            f"{row['Price-cut ratio']} | {row['Price-cut listings']} |"
        )

    lines.append("")
    lines.append("## Rolling 7-day flow")
    lines.append("")
    lines.append("| Segment | Active | Added last 7d | Removed proxy last 7d | Comparable daily pairs |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in weekly_rows:
        lines.append(
            f"| {row['Segment']} | {row['Active']} | {row['Added last 7d']} | "
            f"{row['Removed proxy last 7d']} | {row['Comparable daily pairs']} |"
        )

    lines.append("")
    lines.append("## How to read it")
    lines.append("")
    lines.append("- **Active listings** = current supply visible on tracked brand sites.")
    lines.append("- **Daily new** = homes visible now that were not visible in the prior comparable snapshot.")
    lines.append("- **Daily removed** = homes visible in the prior comparable snapshot that are no longer visible; sell-through proxy, not confirmed sales.")
    lines.append("- **Added last 7d** = unique homes that appeared in any daily comparison during the rolling 7-day window.")
    lines.append("- **Removed proxy last 7d** = unique homes that disappeared in any daily comparison during the rolling 7-day window.")
    lines.append("- **Price-cut ratio** = active listings with a lower price than the prior snapshot or explicit price-cut/savings language.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- New/removed counts only compare brands present in both snapshots, so adding a new tracked builder does not count its entire inventory as newly listed homes.")
    lines.append("- Removed listings are a sell-through proxy. A home can disappear because of sale, relisting, URL changes, or temporary website changes.")
    lines.append("- The weekly flow becomes more useful after at least seven successful daily snapshots.")

    Path(out_path).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate GRBK listing flow report.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    parser.add_argument("--out", default="reports/weekly_report.md")
    args = parser.parse_args()
    generate_report(args.snapshot_dir, args.out)
