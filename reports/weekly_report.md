# GRBK Listing Flow Tracker

Latest snapshot: **2026-05-12**
Daily comparison baseline: **2026-05-08**

## Current and daily metrics

| Segment | Active | Daily new | Daily removed | Price-cut ratio | Price-cut listings |
|---|---:|---:|---:|---:|---:|
| Total tracked | 661 | 18 | 19 | 31.2% | 206 |
| Southgate Homes | 14 | 0 | 0 | 64.3% | 9 |
| Trophy Signature Homes | 340 | 15 | 16 | 27.1% | 92 |
| CB JENI Homes | 47 | N/A | N/A | 74.5% | 35 |
| Normandy Homes | 65 | 1 | 1 | 76.9% | 50 |
| Centre Living Homes | 27 | 0 | 0 | 3.7% | 1 |
| The Providence Group | 131 | 2 | 2 | 0.8% | 1 |
| GHO Homes | 37 | 0 | 0 | 48.6% | 18 |

## Rolling 7-day flow

| Segment | Active | Added last 7d | Removed proxy last 7d | Comparable daily pairs |
|---|---:|---:|---:|---:|
| Total tracked | 661 | 57 | 47 | 4 |
| Southgate Homes | 14 | 1 | 1 | 4 |
| Trophy Signature Homes | 340 | 53 | 43 | 4 |
| CB JENI Homes | 47 | N/A | N/A | 0 |
| Normandy Homes | 65 | 1 | 1 | 1 |
| Centre Living Homes | 27 | 0 | 0 | 1 |
| The Providence Group | 131 | 2 | 2 | 1 |
| GHO Homes | 37 | 0 | 0 | 1 |

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
| Week 3 | 2026-05-08 to 2026-05-14 | Total tracked | 661 | 57 | 47 | 4 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | Southgate Homes | 14 | 1 | 1 | 4 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | Trophy Signature Homes | 340 | 53 | 43 | 4 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | CB JENI Homes | 47 | N/A | N/A | 0 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | Normandy Homes | 65 | 1 | 1 | 1 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | Centre Living Homes | 27 | 0 | 0 | 1 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | The Providence Group | 131 | 2 | 2 | 1 | captured |
| Week 3 | 2026-05-08 to 2026-05-14 | GHO Homes | 37 | 0 | 0 | 1 | captured |

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