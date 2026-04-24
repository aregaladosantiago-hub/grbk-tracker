# GRBK Weekly Inventory & Absorption Tracker

Latest snapshot: **2026-04-24**

## Executive read

This tracker measures public website listing behavior, not official GRBK orders, closings, or backlog. Treat removed listings as a sell-through proxy only after manual QA.

## Top-level metrics

| Metric                                    |        Value |
|:------------------------------------------|-------------:|
| Active listings                           | 54           |
| New listings vs prior week snapshot       | 54           |
| Removed listings vs prior week snapshot   |  0           |
| Possible removals, absent 1+ day          |  0           |
| Probable removals, absent 7+ days         |  0           |
| High-confidence removals, absent 14+ days |  0           |
| Listings with price cuts                  | 32           |
| Listings with incentive language          | 12           |
| Median active listing price               |  1.10997e+06 |
| Median days seen, active listings         |  1           |

## Active inventory by brand

| brand           |   active_listings |   price_cuts |   incentives |    median_price |
|:----------------|------------------:|-------------:|-------------:|----------------:|
| Southgate Homes |                50 |           28 |           12 |      1.1419e+06 |
| Normandy Homes  |                 4 |            4 |            0 | 613598          |

## Interpretation framework

Positive signal: inventory flat/down, removals rising, limited price cuts, limited incentives, ready-now inventory not aging.

Negative signal: inventory building, removals slowing, broader price cuts, more incentives, completed inventory aging.

## QA caveats

- A removed listing is not automatically a sale. It may be under contract, relisted, temporarily hidden, or moved to a new URL.
- The first few weeks are for parser validation; do not use the data in a write-up until the captured listings match the websites manually.
- Brand-specific parsers will likely be needed for cleaner address, community, lot, and plan extraction.
