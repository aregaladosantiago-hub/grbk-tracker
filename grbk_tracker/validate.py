import argparse
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "address",
    "status", "price", "raw_text", "qa_flag"
}


def main():
    parser = argparse.ArgumentParser(description="Validate latest GRBK tracker snapshot quality.")
    parser.add_argument("--snapshot-dir", default="data/snapshots")
    args = parser.parse_args()

    files = sorted(Path(args.snapshot_dir).glob("*.csv"))
    if not files:
        raise SystemExit("No snapshots found yet.")

    latest = files[-1]
    df = pd.read_csv(latest)
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise SystemExit(f"Missing columns in {latest}: {sorted(missing_cols)}")

    print(f"Validated {latest}: {len(df)} rows")
    if len(df) == 0:
        print("WARNING: snapshot has zero rows. This may mean the websites blocked scraping or the parser needs improvement.")
        return

    print("Rows by brand:")
    print(df.groupby("brand").size().sort_values(ascending=False).to_string())
    print("QA flags:")
    print(df["qa_flag"].fillna("clean").value_counts().head(20).to_string())


if __name__ == "__main__":
    main()
