---
title: Runbook - withdrawal queue backlog
authority: company
effective_date: 2026-03-01
owner: Payments SRE
facts:
  runbook.withdrawal_queue.step3: "Step 3: restart the withdrawal signer service."
---
# Runbook: withdrawal queue backlog (synthetic)

Use this runbook when the withdrawal queue depth exceeds 5,000 pending requests for more than 10 minutes.

1. Confirm the alert on the payments dashboard and declare a SEV2 if customer-facing delays exceed 30 minutes.
2. Check the blockchain node health panel for each affected asset.
3. Step 3: restart the withdrawal signer service.
4. Watch queue depth for 15 minutes. If it does not drop, escalate to the payments on-call engineer.
