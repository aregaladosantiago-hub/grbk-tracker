import argparse
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "address",
    "status", "price", "raw_text", "qa_flag"
}


def normalized_text(df: pd.DataFrame, column: str):
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[column].fillna("").astype(str).str.lower().str.strip()


def listing_keys(df: pd.DataFrame):
    return normalized_text(df, "brand") + "|" + normalized_text(df, "market") + "|" + normalized_text(df, "address")


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

    qa_flags = df["qa_flag"].fillna("").astype(str)
    trophy_mismatch = qa_flags.str.contains("trophy_count_mismatch", regex=False)
    if trophy_mismatch.any():
        examples = qa_flags[trophy_mismatch].value_counts().head(5).to_string()
        raise SystemExit(f"Trophy count QA failed. Refusing to trust this snapshot.\n{examples}")

    has_address = normalized_text(df, "address").ne("")
    duplicate_keys = listing_keys(df[has_address])
    duplicates = duplicate_keys[duplicate_keys.duplicated(keep=False)]
    if not duplicates.empty:
        raise SystemExit(
            "Duplicate active listing identities found. This would corrupt active/new/removed metrics.\n"
            + duplicates.value_counts().head(10).to_string()
        )

    print("Rows by brand:")
    print(df.groupby("brand").size().sort_values(ascending=False).to_string())
    print("QA flags:")
    print(df["qa_flag"].fillna("clean").value_counts().head(20).to_string())


if __name__ == "__main__":
    main()
