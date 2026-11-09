---
title: Atlas - withdrawals v2 design
authority: project
effective_date: 2026-08-01
facts:
  atlas.batch_window: "Withdrawals v2 batches on-chain withdrawals every 10 minutes."
---
# Project Atlas: withdrawals v2 design (synthetic)

Atlas replaces per-request on-chain withdrawals with batched transactions to cut fees. Withdrawals v2 batches
on-chain withdrawals every 10 minutes. The batcher reads from the withdrawal queue, groups requests by asset,
signs once per batch, and records the batch id on each request. Rollback switches the queue back to per-request mode.
