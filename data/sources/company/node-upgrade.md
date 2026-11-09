---
title: Runbook - blockchain node upgrade
authority: company
effective_date: 2026-09-01
---
# Runbook: blockchain node upgrade (synthetic)

Upgrade one node at a time. Drain the node from the RPC load balancer, upgrade, and wait for it to reach the
chain tip before returning it to rotation. Never upgrade more than one signer-facing node in the same hour.
After the upgrade, confirm the withdrawal signer can reach the node RPC endpoint.
