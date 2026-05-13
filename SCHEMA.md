# Snapshot schema

Each row is one public listing captured on one date.

| Field | Description |
|---|---|
| snapshot_date | Date the scraper ran |
| brand | GRBK builder brand |
| market | DFW, Texas, Atlanta, Florida Treasure Coast, etc. |
| source_url | Page where listing was found |
| url | Listing/detail URL when available |
| home_key | Stable key used to track the home over time |
| community | Community/neighborhood when extracted |
| address | Listing address when extracted |
| lot | Lot/homesite when extracted |
| plan | Floor plan when extracted |
| status | Ready Now, Quick Move-In, Under Construction, etc. |
| price | Current advertised price |
| prior_price | Prior advertised price when shown |
| sqft | Square footage |
| beds | Bedroom count |
| baths | Bathroom count |
| garage | Garage spaces |
| incentive_text | Promotion/incentive language when detected |
| raw_text | Raw listing-card text for audit |
| qa_flag | Missing address, price, status, etc. |

## Weekly history schema

`reports/weekly_history.csv` stores one row per seven-day period and segment.

| Field | Description |
|---|---|
| week | Sequential period label from the first saved snapshot |
| period_start | First calendar date in that seven-day period |
| period_end | Last calendar date in that seven-day period |
| snapshot_date | Snapshot used as that period's active-listing state |
| segment | Total tracked or builder brand |
| active | Active listings in the period-end snapshot |
| added_last_7d | Unique listings that appeared during the trailing seven-day window |
| removed_proxy_last_7d | Unique listings that disappeared during the trailing seven-day window |
| comparable_daily_pairs | Daily snapshot pairs supporting the added/removed counts |
| status | `captured` when the segment has data, otherwise `no_rows` |
