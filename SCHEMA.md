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
