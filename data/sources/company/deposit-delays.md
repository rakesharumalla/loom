---
title: Runbook - deposit delays
authority: company
effective_date: 2026-08-15
---
# Runbook: deposit delays (synthetic)

If deposits are delayed for an asset, check the node's block height against two public explorers. When the node
lags by more than 20 blocks, fail over to the standby node and open a SEV3.
