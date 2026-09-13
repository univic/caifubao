# Caifubao Kubernetes Examples

This directory is intended for public, example-only Kubernetes assets.

Real deployment overlays, private registry settings, domains, namespaces,
kubeconfigs, and operator runbooks must stay outside the public repository.
For this workspace, those files have been moved under `caifubao-private/`.

Public deployment examples should use placeholder values such as:

- `registry.example.com/your-org/caifubao-backend`
- `registry.example.com/your-org/caifubao-mongodb-tools`
- `caifubao-example`
- `app.example.com`
- `api.example.com`

MongoDB backup examples are intentionally public-safe:

- `base/mongodb-backup.yaml` defines a suspended S3-compatible backup CronJob
  with placeholder object-storage config.
- `base/mongodb-restore-job.example.yaml` is not included in the base
  kustomization because it runs `mongorestore --drop`; copy it into a private
  overlay only when performing an approved restore.
- Real bucket names, endpoints, credentials, retention policy, and restore
  runbooks belong in `caifubao-private`.

Factor-lab research panel examples are public-safe:

- `base/factor-lab-panel.example.yaml` is the ReadWriteOnce claim that holds the
  frozen factor-lab parquet; it is not in the base kustomization because the lab
  is research-only.
- `base/factor-lab-export.example.yaml` and
  `base/factor-lab-evaluate.example.yaml` are the matching Jobs (kept separate so
  an `apply -f` cannot start evaluate before export finishes). Real storage
  classes, node pinning, and the operator runbook belong in `caifubao-private`;
  the workflow is documented in `docs/operations/factor-lab.md`.

Tailscale Kubernetes API server proxy examples are also public-safe:

- `tailscale/api-server-proxy.proxygroup.example.yaml` defines the recommended
  high-availability `ProxyGroup` shape for exposing kube-apiserver inside a
  tailnet.
- `tailscale/api-server-proxy-rbac.example.yaml` shows namespace-scoped RBAC for
  GitHub Actions runners tagged as `tag:ci-deploy`.
- Real tailnet names, OAuth credentials, ACL policy, and private namespaces
  belong in `caifubao-private`.

Before publishing this repository, add sanitized example overlays and update CI
to render those examples instead of private environments.
