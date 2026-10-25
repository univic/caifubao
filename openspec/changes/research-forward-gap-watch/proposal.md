## Why

A certified 120-session Research FORWARD track cannot be manually backfilled:
a missing strategy run is a permanent gap. The daily capture code in #318
tracks individual job attempts, but without an independent session checker a
missing CronJob execution may go unnoticed. Milestone A requires that omissions
be visible and actionable.

## What Changes

- Add a read-only watcher comparing eligible China A-share trade-calendar
  sessions with only exact-spec, immutable \`FORWARD\` + \`COMPLETED\` research
  plans under an ACTIVE certified window.
- Distinguish skipped exchange holidays from missing trading-day evidence,
  reject REPLAY/legacy/wrong-hash rows, and report matched CronJob status
  independently of operator-run sessions.
- Output machine-readable gap evidence; return a nonzero process exit code for
  missing sessions or absent/invalid window and optionally post alerts through
  the existing health-watcher webhook mechanism.
- Publish a suspended-by-default 23:10 Asia/Shanghai Research monitoring
  CronJob template, entirely read-only. Private rollout remains separate.

## Capabilities

### New Capabilities

- \`research-forward-gap-watch\`: fail-loud observability of the 120-session
  candidate FORWARD track.

## Impact

- \`datahub/app/jobs/research_forward_watch.py\`
- \`datahub/app/test/test_research_forward_watch.py\`
- \`k8s/overlays/example-research/datahub-cronjobs.yaml\`

## Non-Goals

A missed trading session is not retroactively promoted by the watcher;
observing one does not authorize closing/reopening a window. This does not
compute account NAV, enable real trades, or send a webhook unless an operator
configures a destination.
