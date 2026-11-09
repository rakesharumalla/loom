---
title: 'Atlas component: Brivika'
authority: project
synthetic: true
---
# Atlas component: Brivika

The Brivika component of withdrawals v2 retries failed signing requests 2 times with exponential backoff before parking them in the Visol dead-letter queue.

Operators should record each action in the incident ticket. Coordinate with the owning team before changing production settings, and confirm the change on the monitoring dashboard. Customer communication goes through Support using the approved macros. If the situation is unclear, escalate to the on-call lead rather than guessing. Review this document quarterly and after every related incident.
