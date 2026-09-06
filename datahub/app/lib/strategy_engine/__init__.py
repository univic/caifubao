# -*- coding: utf-8 -*-
"""Strategy-layer core logic for the paper-first strategy runner.

Pure, dependency-injected selection / rebalance / NAV logic (no DB imports at
module load). The runner layer feeds this engine VERIFIED score predictions,
quote prices, and eligibility marks. Direction semantics live ONLY in the
scoring construction layer; this layer always buys the highest-scored names
("buy high"), per architecture-layers-strategy-design.md §4.3.
"""
