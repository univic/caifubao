# -*- coding: utf-8 -*-
"""Mongo-backed research ledger: registrations, trials, confirmations, verdicts.

The pipeline is storage-agnostic by design; this is the durable adapter. Four
append-only collections:

* ``research_registrations`` — one document per registration, written once. A
  second write with different content is refused, because the criteria are frozen.
* ``research_trials`` — one row per evaluated configuration. Counted per hypothesis
  family, so a new registration cannot reset the budget.
* ``research_confirmations`` — one row per confirmation, capped per family.
* ``research_verdicts`` — content-addressed by ``_id = verdict_hash``, so an edit
  changes the address instead of rewriting a record.

Nothing here deletes. A registration that should stop is retired by a verdict, not
by removing its history.

This adapter never decides anything: it records what the pipeline decided.
"""

from __future__ import annotations

from datetime import datetime, timezone

from caifubao_contracts.strategy_research import (
    validate_confirmation,
    validate_registration,
    validate_verdict,
    verdict_hash,
)

REGISTRATIONS = "research_registrations"
TRIALS = "research_trials"
CONFIRMATIONS = "research_confirmations"
VERDICTS = "research_verdicts"


class ResearchStoreError(ValueError):
    """The durable store refused a write; the caller must not treat it as a retry."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MongoResearchStore:
    """Durable research ledger over a pymongo database handle."""

    def __init__(self, db):
        self.db = db

    # -- registrations ---------------------------------------------------
    def get_registration(self, registration_id: str) -> dict | None:
        document = self.db[REGISTRATIONS].find_one({"_id": registration_id})
        if document is None:
            return None
        document.pop("_id", None)
        return document

    def add_registration(
        self, registration: dict, *, proof: dict | None = None
    ) -> None:
        validated = validate_registration(registration)
        registration_id = validated["registration_id"]
        existing = self.get_registration(registration_id)
        if existing is not None:
            # Compare only the declared fields: the stored record also carries
            # bookkeeping (created_at, the calendar proof), which must not look
            # like a criteria change.
            if {key: existing.get(key) for key in validated} != validated:
                raise ResearchStoreError(
                    "registration criteria are frozen; create a new registration id"
                )
            if proof is not None and not existing.get("calendar_proof"):
                # Filling a missing attestation is not a criteria change.
                self.db[REGISTRATIONS].update_one(
                    {"_id": registration_id},
                    {"$set": {"calendar_proof": dict(proof)}},
                )
            return
        document = dict(validated, _id=registration_id, created_at=_now())
        if proof is not None:
            document["calendar_proof"] = dict(proof)
        try:
            self.db[REGISTRATIONS].insert_one(document)
        except Exception as exc:  # duplicate key from a concurrent writer
            raise ResearchStoreError(
                f"registration {registration_id!r} already exists"
            ) from exc

    def registration_calendar_proof(self, registration_id: str) -> dict | None:
        document = self.db[REGISTRATIONS].find_one(
            {"_id": registration_id}, {"calendar_proof": 1}
        )
        if document is None:
            return None
        return document.get("calendar_proof")

    # -- trials ----------------------------------------------------------
    def add_trial(self, registration: dict) -> int:
        family = registration["family"]
        self.db[TRIALS].insert_one(
            {
                "registration_id": registration["registration_id"],
                "family": family,
                "recorded_at": _now(),
            }
        )
        return self.db[TRIALS].count_documents({"family": family})

    def family_trials(self, family: str) -> int:
        return self.db[TRIALS].count_documents({"family": family})

    def registration_trials(self, registration_id: str) -> int:
        return self.db[TRIALS].count_documents({"registration_id": registration_id})

    # -- confirmations ---------------------------------------------------
    def add_confirmation(self, registration: dict, record: dict) -> int:
        family = registration["family"]
        validated = validate_confirmation(record)
        self.db[CONFIRMATIONS].insert_one(
            dict(
                validated,
                registration_id=registration["registration_id"],
                family=family,
                recorded_at=_now(),
            )
        )
        return self.db[CONFIRMATIONS].count_documents({"family": family})

    def confirmations_for(self, registration_id: str) -> list[dict]:
        documents = self.db[CONFIRMATIONS].find({"registration_id": registration_id})
        records = []
        for document in documents:
            record = {key: value for key, value in document.items() if key != "_id"}
            records.append(record)
        return records

    def family_confirmations(self, family: str) -> int:
        return self.db[CONFIRMATIONS].count_documents({"family": family})

    def registration_confirmations(self, registration_id: str) -> int:
        return self.db[CONFIRMATIONS].count_documents(
            {"registration_id": registration_id}
        )

    # -- verdicts --------------------------------------------------------
    def add_verdict(self, verdict: dict) -> str:
        validated = validate_verdict(verdict)
        digest = verdict_hash(validated)
        existing = self.db[VERDICTS].find_one({"_id": digest})
        if existing is not None:
            stored = {key: value for key, value in existing.items() if key != "_id"}
            if stored != validated:
                raise ResearchStoreError(
                    "verdict identity collision: same address, different content"
                )
            return digest
        self.db[VERDICTS].insert_one(dict(validated, _id=digest))
        return digest

    def get_verdict(self, digest: str) -> dict | None:
        document = self.db[VERDICTS].find_one({"_id": digest})
        if document is None:
            return None
        document.pop("_id", None)
        return document
