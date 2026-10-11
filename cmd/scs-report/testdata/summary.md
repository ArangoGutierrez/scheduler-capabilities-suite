# Scheduling capabilities: documented vs observed

Run fixture-run. Each verdict comes from a single run: evidence for the POC, not a result to quote.

| capability | profile | documented | observed | agree | evidence |
|---|---|---|---|---|---|
| gang | vanilla | supported | supported | yes | no gang was partly placed for more than 5 s, and all 2 gangs completed |
| priority | vanilla | supported | not_supported | no | all 1 queued lower-priority pods were scheduled before the high-priority pod |

## Findings

- vanilla, priority: documented supported, observed not_supported (all 1 queued lower-priority pods were scheduled before the high-priority pod).

## Controls

- vanilla, gang-control: partial (gang gang-b was partly placed for 21.5 s; all 2 gangs completed).
