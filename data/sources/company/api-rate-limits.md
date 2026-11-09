---
title: Public API rate limits
authority: company
effective_date: 2026-05-01
---
# Public API rate limits (synthetic)

Public REST endpoints allow 10 requests per second per API key. Private endpoints allow 15. Bursts above the
limit return HTTP 429 with a Retry-After header.
