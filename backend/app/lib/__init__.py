"""Backend internal libraries.

This package only contains modules the Flask API actually uses. The legacy
general worker/processor pipeline (and the shadow data-ingestion copy) was
removed in the architecture-review cleanup: the backend must not run scheduled
collection, factor, or signal jobs (see RULES.md module boundaries).
"""
