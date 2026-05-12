import argparse
import json
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


def load_configured_brands(config_path: str):
    path = Path(config_path)
    if not path.exists():
        return []

    with open(path, "r") as f:
        cfg = json.load(f)

    brands = []
    for brand_cfg in cfg.get("brands", []):
        brand = brand_cfg.get("brand")
        if brand and brand not in brands:
            brands.append(brand)
    return brands


def report_brands(df: pd.DataFrame, configured_brands):
    seen = list(configured_brands)
    if "brand" in df.columns:
        for brand in sorted(df["brand"].dropna().unique()):
            if brand not in seen:
                seen.append(brand)
    return seen


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
        "New since prior": "N/A" if new is None else new,
        "Removed / Sold Proxy": "N/A" if removed is None else removed,
        "Net Change": net_change(new, removed),
    }, new_keys, removed_keys


def snapshot_for_date(df: pd.DataFrame, snapshot_date):
    return df[df["snapshot_date"] == snapshot_date].copy()


def brand_slice(df: pd.DataFrame, brand: str):
    if df.empty or "brand" not in df.columns:
        return df.iloc[0:0].copy()
    return df[normalized_text(df, "brand") == brand.lower()].copy()


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


def net_change(added, removed):
    if not isinstance(added, int) or not isinstance(removed, int):
        return "N/A"
    value = added - removed
    if value > 0:
        return f"+{value}"
    return str(value)


def flow_row_for_period(label, df: pd.DataFrame, dates, period_start, snapshot_date, brand: str = None):
    latest = snapshot_for_date(df, snapshot_date)
    if brand is not None:
        latest = brand_slice(latest, brand)

    active = len(unique_keys(latest))
    period_start = pd.Timestamp(period_start).normalize()
    snapshot_date = pd.Timestamp(snapshot_date).normalize()
    period_dates = [d for d in dates if period_start <= pd.Timestamp(d).normalize() <= snapshot_date]

    added_keys = set()
    removed_keys = set()
    comparable_pairs = 0

    for current_date in period_dates:
        current_index = dates.index(current_date)
        if current_index == 0:
            continue
        previous_date = dates[current_index - 1]
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
        "Added this week": added,
        "Removed / Sold Proxy": removed,
        "Net Change": net_change(added, removed),
        "Comparable daily pairs": comparable_pairs,
    }


def current_week_period(dates, latest_date):
    first_date = pd.Timestamp(dates[0]).normalize()
    latest = pd.Timestamp(latest_date).normalize()
    week_number = int((latest - first_date).days // 7) + 1
    period_start = first_date + pd.Timedelta(days=(week_number - 1) * 7)
    period_end = period_start + pd.Timedelta(days=6)
    return week_number, period_start, period_end


def daily_flow_rows_for_period(df: pd.DataFrame, dates, period_start, snapshot_date):
    rows = []
    period_start = pd.Timestamp(period_start).normalize()
    snapshot_date = pd.Timestamp(snapshot_date).normalize()
    period_dates = [d for d in dates if period_start <= pd.Timestamp(d).normalize() <= snapshot_date]

    for current_date in period_dates:
        current_index = dates.index(current_date)
        if current_index == 0:
            continue
        previous_date = dates[current_index - 1]
        previous = snapshot_for_date(df, previous_date)
        current = snapshot_for_date(df, current_date)
        flow = flow_keys_between(previous, current)
        if flow is None:
            continue
        added, removed = flow
        rows.append({
            "Date": pd.Timestamp(current_date).date().isoformat(),
            "Added": len(added),
            "Removed / Sold Proxy": len(removed),
            "Net Change": net_change(len(added), len(removed)),
        })
    return rows


def weekly_checkpoint_dates(dates):
    first_date = pd.Timestamp(dates[0]).normalize()
    checkpoints = {}
    for raw_date in dates:
        snapshot_date = pd.Timestamp(raw_date).normalize()
        week_number = int((snapshot_date - first_date).days // 7) + 1
        checkpoints[week_number] = max(snapshot_date, checkpoints.get(week_number, snapshot_date))

    rows = []
    for week_number, snapshot_date in sorted(checkpoints.items()):
        period_start = first_date + pd.Timedelta(days=(week_number - 1) * 7)
        period_end = period_start + pd.Timedelta(days=6)
        rows.append((week_number, period_start, period_end, snapshot_date))
    return rows


def data_status(active, comparable_pairs):
    if active or comparable_pairs:
        return "captured"
    return "no_rows"


def build_weekly_history(df: pd.DataFrame, dates, brands):
    rows = []
    for week_number, period_start, period_end, snapshot_date in weekly_checkpoint_dates(dates):
        segment_specs = [("Total tracked", None)] + [(brand, brand) for brand in brands]
        for label, brand in segment_specs:
            row = flow_row_for_period(label, df, dates, period_start, snapshot_date, brand=brand)
            rows.append({
                "week": f"Week {week_number}",
                "period_start": period_start.date().isoformat(),
                "period_end": period_end.date().isoformat(),
                "snapshot_date": snapshot_date.date().isoformat(),
                "segment": label,
                "active": row["Active"],
                "added_this_week": row["Added this week"],
                "removed_sold_proxy": row["Removed / Sold Proxy"],
                "net_change": row["Net Change"],
                "status": data_status(row["Active"], row["Comparable daily pairs"]),
            })
    return pd.DataFrame(rows)


def generate_report(snapshot_dir: str, out_path: str, config_path: str = "config/brands.json"):
    df, paths = load_snapshots(snapshot_dir)
    configured_brands = load_configured_brands(config_path)
    if df.empty:
        Path(out_path).write_text("# GRBK Listing Flow Tracker\n\nNo listing rows captured yet.\n")
        return

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    dates = sorted(d for d in df["snapshot_date"].dropna().unique())
    brands = report_brands(df, configured_brands)

    latest_date = dates[-1]
    latest = snapshot_for_date(df, latest_date)

    prior = pd.DataFrame()
    prior_date = None
    if len(dates) >= 2:
        prior_date = dates[-2]
        prior = snapshot_for_date(df, prior_date)

    total_row, total_new_keys, total_removed_keys = metric_row("Total tracked", latest, prior)
    current_week_number, current_period_start, current_period_end = current_week_period(dates, latest_date)

    brand_rows = []
    weekly_rows = [flow_row_for_period("Total tracked", df, dates, current_period_start, latest_date)]
    for brand in brands:
        brand_latest = brand_slice(latest, brand)
        brand_prior = brand_slice(prior, brand)
        row, _, _ = metric_row(brand, brand_latest, brand_prior)
        brand_rows.append(row)
        weekly_rows.append(flow_row_for_period(brand, df, dates, current_period_start, latest_date, brand=brand))
    daily_rows = daily_flow_rows_for_period(df, dates, current_period_start, latest_date)

    reports = Path("reports")
    reports.mkdir(exist_ok=True)
    latest.to_csv(reports / "latest_active.csv", index=False)
    weekly_history = build_weekly_history(df, dates, brands)
    weekly_history.to_csv(reports / "weekly_history.csv", index=False)

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
    lines.append(
        f"Current week: **Week {current_week_number} "
        f"({current_period_start.date()} to {current_period_end.date()})**"
    )
    lines.append("")
    lines.append("## Current snapshot movement")
    lines.append("")
    lines.append("| Segment | Active | New since prior | Removed / Sold Proxy | Net Change |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in [total_row] + brand_rows:
        lines.append(
            f"| {row['Segment']} | {row['Active']} | {row['New since prior']} | "
            f"{row['Removed / Sold Proxy']} | {row['Net Change']} |"
        )

    lines.append("")
    lines.append("## Weekly sales traction")
    lines.append("")
    lines.append("| Segment | Active | Added this week | Removed / Sold Proxy | Net Change |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in weekly_rows:
        lines.append(
            f"| {row['Segment']} | {row['Active']} | {row['Added this week']} | "
            f"{row['Removed / Sold Proxy']} | {row['Net Change']} |"
        )

    lines.append("")
    lines.append("## Daily movement this week")
    lines.append("")
    if daily_rows:
        lines.append("| Date | Added | Removed / Sold Proxy | Net Change |")
        lines.append("|---|---:|---:|---:|")
        for row in daily_rows:
            lines.append(
                f"| {row['Date']} | {row['Added']} | {row['Removed / Sold Proxy']} | "
                f"{row['Net Change']} |"
            )
    else:
        lines.append("No comparable daily movement rows yet for the current week.")

    lines.append("")
    lines.append("## Weekly history log")
    lines.append("")
    lines.append("| Week | Period | Segment | Active | Added this week | Removed / Sold Proxy | Net Change | Status |")
    lines.append("|---|---|---|---:|---:|---:|---:|---|")
    for row in weekly_history.to_dict("records"):
        period = f"{row['period_start']} to {row['period_end']}"
        lines.append(
            f"| {row['week']} | {period} | {row['segment']} | {row['active']} | "
            f"{row['added_this_week']} | {row['removed_sold_proxy']} | "
            f"{row['net_change']} | {row['status']} |"
        )

    lines.append("")
    lines.append("## How to read it")
    lines.append("")
    lines.append("- **Active listings** = current supply visible on tracked brand sites.")
    lines.append("- **New since prior** = homes visible now that were not visible in the prior comparable snapshot.")
    lines.append("- **Removed / Sold Proxy** = homes visible in the prior comparable snapshot that are no longer visible; useful for sales traction, but not confirmed sold.")
    lines.append("- **Added this week** = unique homes that appeared during the current fixed weekly period.")
    lines.append("- **Net Change** = added minus removed/sold-proxy homes.")
    lines.append("- **Daily movement this week** = total added/removed movement for each successful snapshot inside the current weekly period.")
    lines.append("- **Weekly history log** = frozen weekly periods from the first saved snapshot, so Week 1 and Week 2 remain auditable instead of being overwritten.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- New/removed counts only compare brands present in both snapshots, so adding a new tracked builder does not count its entire inventory as newly listed homes.")
    lines.append("- Newly configured brands show `no_rows` until their first successful snapshot is captured.")
    lines.append("- A removed listing can mean sale, relisting, URL change, or a temporary website change, so use it as a directional sales-traction signal.")
    lines.append("- If a scheduled scrape is missed, the next daily movement row covers the change since the prior successful snapshot.")
    lines.append("- Price-cut metrics were removed from the headline report because they measure builder discounting, not weekly sales traction.")

    Path(out_path).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate GRBK listing flow report.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    parser.add_argument("--out", default="reports/weekly_report.md")
    parser.add_argument("--config", default="config/brands.json")
    args = parser.parse_args()
    generate_report(args.snapshot_dir, args.out, args.config)
