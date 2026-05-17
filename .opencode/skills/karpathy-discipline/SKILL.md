---
name: karpathy-discipline
description: Behavioral guidelines to reduce LLM coding mistakes, derived from Andrej Karpathy's observations. Covers assumption-checking, simplicity-first, surgical changes, and goal-driven execution.
license: MIT
compatibility: opencode
metadata:
  audience: all
  project: caifubao
  source-repo: https://github.com/multica-ai/andrej-karpathy-skills
---

## What this skill covers

Four behavioral principles adapted from Andrej Karpathy's observations on LLM coding
pitfalls. These guidelines apply to ALL agents working in caifubao, regardless of
role (orchestrator, implementer, reviewer). They complement the existing module-boundary
and spec-gate rules in AGENTS.md.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks
(simple typo fixes, one-line changes, formatting), use judgment — not every change
needs the full rigor.

---

## Principle 1: Think Before Coding

> "Don't assume. Don't hide confusion. Surface tradeoffs."
>
> LLMs often pick an interpretation silently and run with it. This principle forces
> explicit reasoning before any code is written.

### Rules

- **State assumptions explicitly.** Before implementing, write down what you are
  assuming about the codebase, the data, the user's intent, and the existing behavior.
  Add an `Assumptions:` field to your task notes.
- **Present multiple interpretations.** If the request is ambiguous, list 2-3 possible
  interpretations and ask the orchestrator to pick one. Never silently choose.
- **Push back when warranted.** If a simpler approach exists, or if the request
  conflicts with module boundaries or spec contracts, say so before coding.
- **Stop when confused.** If you encounter code or behavior you don't understand,
  stop and name what's unclear. Do not guess an implementation and hope it works.

### Self-check

- [ ] Did I write down my assumptions before writing code?
- [ ] If something was ambiguous, did I ask rather than guess?
- [ ] If I noticed a conflict with existing boundaries, did I flag it?

---

## Principle 2: Simplicity First

> "Minimum code that solves the problem. Nothing speculative."
>
> Combat the LLM tendency to over-engineer: abstractions for single-use code,
> configurability that wasn't requested, error handling for impossible scenarios.

### Rules

- **No features beyond what was asked.** If the task says "add a volume_ratio factor",
  do not also add a volume_profile factor "because it's related".
- **No abstractions for single-use code.** A standalone function is fine. It does not
  need a class hierarchy, a factory pattern, or a plugin system.
- **No "flexibility" that wasn't requested.** Do not add config knobs, environment
  variables, or CLI flags unless the task explicitly calls for them.
- **No error handling for impossible scenarios.** Handle real edge cases (missing data,
  network errors). Do not handle hypothetical future scenarios.
- **If 200 lines could be 50, rewrite it.** Before submitting, ask: "Would a senior
  engineer say this is overcomplicated?" If yes, simplify.

### caifubao-specific

- Prefer the existing patterns in `datahub/app/lib/scoring_engine/` over new patterns.
- A new factor component is one function returning a dict — it does not need its own module.
- An API endpoint is one Flask route function — it does not need a service layer unless
  the logic is shared across 3+ endpoints.

### Self-check

- [ ] Can I delete any line without losing behavior that was explicitly requested?
- [ ] Did I add any abstraction (class, factory, plugin) that serves only one caller?
- [ ] Would a senior engineer reading this say "this is overcomplicated"?

---

## Principle 3: Surgical Changes

> "Touch only what you must. Clean up only your own mess."
>
> LLMs have a tendency to "improve" adjacent code, reformat files, delete comments
> they don't fully understand, or refactor things that aren't broken.

### Rules

- **Don't "improve" adjacent code, comments, or formatting.** If a file has mixed
  quote styles and your task is to add a function, add it in the style of the
  surrounding code. Do not reformat the whole file.
- **Don't refactor things that aren't broken.** If a helper function works, leave it
  alone — even if you'd write it differently.
- **Match existing style, even if you'd do it differently.** The codebase uses
  single quotes? Use single quotes. Uses `snake_case`? Use `snake_case`.
- **If you notice unrelated dead code, mention it — don't delete it.** Dead code
  removal is a separate task that needs its own review.
- **Clean up only your own orphans.** Remove imports, variables, or functions that
  YOUR changes made unused. Do not remove pre-existing dead code unless asked.

### The test

Every changed line should trace directly to the user's request. If you cannot point
to the request and say "this line is necessary because...", it should not be in the diff.

### Self-check

- [ ] Does every changed line have a direct connection to the requested outcome?
- [ ] Did I reformat, reorder, or "improve" any code I wasn't asked to touch?
- [ ] Did I remove any pre-existing code, comments, or imports that my changes didn't orphan?

---

## Principle 4: Goal-Driven Execution

> "Define success criteria. Loop until verified."
>
> LLMs are exceptionally good at looping until they meet specific goals. Weak criteria
> ("make it work") require constant clarification. Strong criteria let the agent
> iterate independently.

### Rules

Transform imperative tasks into verifiable goals:

| Instead of... | Transform to... |
|--------------|----------------|
| "Add validation" | "Write tests for invalid inputs, then make them pass" |
| "Fix the bug" | "Write a test that reproduces the bug, then make it pass" |
| "Refactor X" | "Ensure all existing tests pass before and after the refactor" |
| "Add a factor" | "Write a FakeQuote test that asserts the factor value at 3 known data points, then implement" |

For multi-step tasks, state a brief plan with verification checkpoints:

```text
1. Write failing test for new component → verify: test fails with expected error
2. Implement component function → verify: test passes
3. Add component to service → verify: full test suite passes
4. Update spec → verify: spec-guardian confirms Scenarios are sufficient
5. Update frontend types → verify: npm run build succeeds
```

### caifubao-specific verification defaults

- Python changes: `ruff check` + `ruff format --check` + smallest useful pytest target
- API changes: focused pytest under `backend/app/test/`
- Datahub changes: focused datahub tests or runner dry-runs
- Frontend changes: `npm run lint` + `npm run build`
- Deployment changes: `kubectl kustomize` or equivalent render check

### Self-check

- [ ] Did I define a verifiable success criterion before writing code?
- [ ] If fixing a bug, did I write a test that reproduces it first?
- [ ] Did my verification loop actually pass, or did I stop at "looks right"?
- [ ] If verification failed, did I iterate until it passed?

---

## Quick Reference Card

```
Assume nothing → Ask when unclear → Surface tradeoffs
Simplify ruthlessly → Delete speculative code → No single-use abstractions
Touch only what's needed → Match existing style → Clean only your own orphans
Define success criteria → Write failing test → Loop until verified
```

## How to Know It's Working

These guidelines are working if you see:

- **Fewer unnecessary changes in diffs** — only requested changes appear
- **Fewer rewrites due to overcomplication** — code is simple the first time
- **Clarifying questions come before implementation** — not after mistakes
- **Clean, minimal PRs** — no drive-by refactoring
- **Tests pass on first or second attempt** — not after dozens of debugging rounds
