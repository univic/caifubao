# Caifubao Kubernetes Examples

This directory is intended for public, example-only Kubernetes assets.

Real deployment overlays, private registry settings, domains, namespaces,
kubeconfigs, and operator runbooks must stay outside the public repository.
For this workspace, those files have been moved under `caifubao-private/`.

Public deployment examples should use placeholder values such as:

- `registry.example.com/your-org/caifubao-backend`
- `caifubao-example`
- `app.example.com`
- `api.example.com`

Before publishing this repository, add sanitized example overlays and update CI
to render those examples instead of private environments.

