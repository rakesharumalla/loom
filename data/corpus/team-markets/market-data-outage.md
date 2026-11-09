---
title: Market data feed outage playbook (Markets team)
authority: team
effective_date: 2026-07-12
---
# Market data feed outage playbook (synthetic)

When the primary market data feed lags more than 2 seconds, fail over to the secondary vendor feed and mark
prices as indicative on the trading UI. Pause new limit orders on affected pairs if both feeds lag. Notify the
Trading Ops lead and log the incident with feed timestamps.
