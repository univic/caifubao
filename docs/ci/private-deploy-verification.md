# Cross-repository private deploy verification (CI)

The public CI workflow `.github/workflows/ci.yml` optionally dispatches private manifest validation to `univic/caifubao-private`, which owns the deployment overlays. The protected `Required Checks` aggregator must never report a failed/unobserved required deploy validation as green.

## Trigger budget

`Detect Changed Areas` exposes a `deploy_contract` flag matching changes in:

- public `k8s/**`
- backend, frontend, datahub, compute-worker and MongoDB-tools Dockerfiles
- the main `ci.yml` or deploy workflow YAML

Other application source-only PRs continue running their appropriate tests and lint jobs. They skip the private manifest render **because neither public manifests nor build/deploy contracts changed**. The actual private deploy workflows retain independent verify/preflight gates whenever a deployment is requested. This path filter is not an exemption for deploy-contract changes. Do not remove their required checks to work around hosted quota.

The default `Required Checks` job still rejects a failed or cancelled private-validation job, and only accepts legitimately skipped jobs for path-independent changes.

## Dispatch/run correlation

1. The public check generates `request_id=public-ci-<GITHUB_RUN_ID>-<GITHUB_RUN_ATTEMPT>`.
2. Dispatch sends this marker together with the exact public SHA in `client_payload`.
3. The private `deploy-dry-run.yml` publishes `Deploy dry-run [<request_id>] for <public_ref>` in `run-name`.
4. Public CI lists private workflow runs and selects **only the exact matching display title**. More than one match, API authorization failures or a missing match fail the check. A matched run must itself complete successfully under `gh run watch --exit-status`.

**Migration order:** merge the paired private-repo change first so `run-name` supports `request_id`; only then merge this public CI change. While the private PR is still pending, a public CI run from this branch is expected to fail closed with “No matching run” rather than silently claiming success. The private run-name change remains compatible with the old public dispatcher, which does not send a marker.

GitHub token `PRIVATE_REPO_DISPATCH_TOKEN` must have access to dispatch into the private repository and **Actions: read** on that repo. Never log token contents or treat missing read permissions as success.

## Regression checklist

- [ ] `backend/app/**` or `frontend/src/**` only: relevant public CI runs, private dry-run is `skipped`.
- [ ] `k8s/**`, a covered Dockerfile, or `ci.yml`: private dry-run runs and is required.
- [ ] Parallel PRs each create distinct `request_id`; their public CI jobs cannot match the other run.
- [ ] Private Actions read revoked: public job fails.
- [ ] Private job fails before runner allocation, is queued too long, or reports manifest failure: public job stays failed/non-success and `Required Checks` blocks.
- [ ] Private job completes the `Verify rendered manifest contracts` step for the matching SHA and succeeds: public job can succeed.
- [ ] No downstream K8s deployment, paper trading or live trading occurs during these CI checks.

Private runner provisioning, smoke testing and variable-based selection are documented in the private repo `docs/ci/private-actions-runner-cutover.md`; see private issue #102.

This change does not provision a machine, read the owner's billing metrics, modify GitHub variables or grant deployment authorization.
