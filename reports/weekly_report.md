# GRBK Listing Flow Tracker

Latest snapshot: **2026-05-07**
Daily comparison baseline: **2026-05-06**

## Current and daily metrics

| Segment | Active | Daily new | Daily removed | Price-cut ratio | Price-cut listings |
|---|---:|---:|---:|---:|---:|
| Total tracked | 347 | 13 | 9 | 38.9% | 135 |
| Southgate Homes | 14 | 0 | 0 | 50.0% | 7 |
| Trophy Signature Homes | 333 | 13 | 9 | 38.4% | 128 |
| CB JENI Homes | 0 | N/A | N/A | 0.0% | 0 |
| Normandy Homes | 0 | N/A | N/A | 0.0% | 0 |
| Centre Living Homes | 0 | N/A | N/A | 0.0% | 0 |
| The Providence Group | 0 | N/A | N/A | 0.0% | 0 |
| GHO Homes | 0 | N/A | N/A | 0.0% | 0 |

## Rolling 7-day flow

| Segment | Active | Added last 7d | Removed proxy last 7d | Comparable daily pairs |
|---|---:|---:|---:|---:|
| Total tracked | 347 | 38 | 54 | 7 |
| Southgate Homes | 14 | 1 | 2 | 7 |
| Trophy Signature Homes | 333 | 37 | 52 | 7 |
| CB JENI Homes | 0 | N/A | N/A | 0 |
| Normandy Homes | 0 | N/A | N/A | 0 |
| Centre Living Homes | 0 | N/A | N/A | 0 |
| The Providence Group | 0 | N/A | N/A | 0 |
| GHO Homes | 0 | N/A | N/A | 0 |

## Weekly history log

| Week | Period | Segment | Active | Added last 7d | Removed proxy last 7d | Comparable daily pairs | Status |
|---|---|---|---:|---:|---:|---:|---|
| Week 1 | 2026-04-24 to 2026-04-30 | Total tracked | 363 | 0 | 0 | 3 | captured |
| Week 1 | 2026-04-24 to 2026-04-30 | Southgate Homes | 15 | 0 | 0 | 3 | captured |
| Week 1 | 2026-04-24 to 2026-04-30 | Trophy Signature Homes | 348 | N/A | N/A | 0 | captured |
| Week 1 | 2026-04-24 to 2026-04-30 | CB JENI Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 1 | 2026-04-24 to 2026-04-30 | Normandy Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 1 | 2026-04-24 to 2026-04-30 | Centre Living Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 1 | 2026-04-24 to 2026-04-30 | The Providence Group | 0 | N/A | N/A | 0 | no_rows |
| Week 1 | 2026-04-24 to 2026-04-30 | GHO Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 2 | 2026-05-01 to 2026-05-07 | Total tracked | 347 | 38 | 54 | 7 | captured |
| Week 2 | 2026-05-01 to 2026-05-07 | Southgate Homes | 14 | 1 | 2 | 7 | captured |
| Week 2 | 2026-05-01 to 2026-05-07 | Trophy Signature Homes | 333 | 37 | 52 | 7 | captured |
| Week 2 | 2026-05-01 to 2026-05-07 | CB JENI Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 2 | 2026-05-01 to 2026-05-07 | Normandy Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 2 | 2026-05-01 to 2026-05-07 | Centre Living Homes | 0 | N/A | N/A | 0 | no_rows |
| Week 2 | 2026-05-01 to 2026-05-07 | The Providence Group | 0 | N/A | N/A | 0 | no_rows |
| Week 2 | 2026-05-01 to 2026-05-07 | GHO Homes | 0 | N/A | N/A | 0 | no_rows |

## How to read it

- **Active listings** = current supply visible on tracked brand sites.
- **Daily new** = homes visible now that were not visible in the prior comparable snapshot.
- **Daily removed** = homes visible in the prior comparable snapshot that are no longer visible; sell-through proxy, not confirmed sales.
- **Added last 7d** = unique homes that appeared in any daily comparison during the rolling 7-day window.
- **Removed proxy last 7d** = unique homes that disappeared in any daily comparison during the rolling 7-day window.
- **Weekly history log** = frozen seven-day periods from the first saved snapshot, so Week 1 and Week 2 remain auditable instead of being overwritten by the latest rolling view.
- **Price-cut ratio** = active listings with a lower price than the prior snapshot or explicit price-cut/savings language.

## Notes

- New/removed counts only compare brands present in both snapshots, so adding a new tracked builder does not count its entire inventory as newly listed homes.
- Newly configured brands show `no_rows` until their first successful snapshot is captured.
- Removed listings are a sell-through proxy. A home can disappear because of sale, relisting, URL changes, or temporary website changes.
- The weekly flow becomes more useful after at least seven successful daily snapshots.