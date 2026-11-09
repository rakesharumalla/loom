---
title: Borealis - market data feed architecture
authority: project
effective_date: 2026-06-20
---
# Project Borealis: feed architecture (synthetic)

Borealis consolidates three market data vendors into one normalized stream. Each vendor feed is parsed by an
adapter, timestamped at ingress, and published to the order book service. The consolidator drops ticks older than
500 milliseconds and records vendor lag per pair.
