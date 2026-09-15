## Why

`stock_industry` is the only datahub collection that stores stock codes in
baostock's separated form (`sh.600000`). Quotes, factors, signals, predictions
and every other consumer use the canonical joined form (`sh600000`).

Consequences, all verified against the live dev and source databases:

- `industry_momentum_component` looks up `StockIndustryClassification.objects(
  stock_code=stock_code)` with the caller's canonical code, so it has **never**
  matched: the component silently returns the neutral fallback (weight 5 of the
  horizon score) for every stock.
- `aggregate_industry_metrics` groups predictions by classification and
  therefore grouped nothing: `industry_daily_metrics` holds **0 documents** in
  both databases, so the component had no upstream data either.
- Any path that needs industry context for a canonical code - the P2a
  `industry_classification` snapshot, `strategy_runner`'s industry constraint,
  the H20 snapshot's industry column - receives "no industry".
- The P3 single-stock baseline found no industry row for `sh600036` in its
  universe artifact. The row exists; it is keyed `sh.600036`.

This is a key-format inconsistency, not missing data: `stock_industry` holds
5,212 classifications. The scoring weights, thresholds and formula are not
wrong; they were fed a lookup that could not match.

## What Changes

- **Ingestion normalizes.** `sync_industry_classification` converts the
  baostock code (`sh.600000`) to the canonical form (`sh600000`) before lookup
  and persistence, mirroring the existing normalization in `stock_quote.py`.
- **One-time migration for stored rows.** A new operator command rewrites the
  legacy separated keys (`^[a-z]{2}\.\d{6}$`) in `stock_industry` to canonical
  form. It is idempotent, supports a write-free dry-run, reports keys that
  match neither shape instead of guessing, leaves `last_synced_at` untouched,
  and merges deterministically when a canonical record already exists.
- **Replay stays point-in-time safe.** Activating a lookup that was dead also
  activates a look-ahead risk: the classification store keeps one current row
  per stock, so replaying an old date would attribute today's industry to it.
  The point-in-time guard already used by the paper strategy moves to
  `app.lib.scoring_engine.industry_pit` — a datahub-only module, because the
  image copies `backend/app/model/` over `datahub/app/model/` — and now accepts
  the plain row mappings the research exporters use. It is applied by the
  per-day industry prefetch, the `industry_momentum` component,
  `aggregate_industry_metrics`, and the H20 autoresearch snapshot export.
- **Contract made explicit.** The industry classification spec records that
  `stock_industry.stock_code` uses the same canonical code as quotes, factors,
  signals and predictions, and the stock-scoring replay requirement records the
  point-in-time attribution rule.
- No change to industry source, CSRC L1/L2 parsing, field names, monthly sync
  cadence, scoring weights, thresholds, formula or `DEFAULT_MODEL_VERSION`.

## Impact

Once the fixed image is deployed and the migration has run, the next scoring
session aggregates real `industry_daily_metrics`; the `industry_momentum`
component becomes non-neutral from the following session, because it only reads
metrics strictly before the scoring date. Scores therefore change from that
point on. Existing stored predictions are not rewritten, and REPLAY rows are
not relabelled: this is a forward correction, not a backfill.

No model version bump is taken here. The archived rule that requires a bump and
a full-market calibration comparison is about changing thresholds or weights;
this change touches neither, and it only makes resolvable an input the existing
configuration already declares (`industry_momentum`, weight 5). Because scores
are still not comparable across the correction boundary, calibration or
comparison windows must be split at it, and a corrected historical
full-market replay stays a separate operator-authorised task.

Rollout order is part of the change, not an operational detail: `stock_industry`
is a full prod-to-dev snapshot synced and upserted by `stock_code`, so
migrating dev while the owning environment still stores the separated key would
simply reinsert it on the next sync. Fix and migrate the owning environment
first, then let dev re-sync and migrate it, and verify that no separated keys
remain in either.

## Non-goals

- No backfill of historical `industry_daily_metrics` and no recomputation of
  historical predictions.
- No change to the CSRC classification source, its parsing, or the legacy
  `*_sw_*` field names.
- No dual-format read compatibility: the storage invariant is canonical keys
  only, so a second accepted form is explicitly rejected.
- No fix of the unrelated pre-existing `--force-update` dead branch in the
  industry sync; it is noted for a separate change.
- No trading, promotion, or execution capability.

