# Kubernetes Overlays

Private `development`, `production`, and service-specific overlays were moved
to `caifubao-private/k8s/`.

The public repository contains sanitized example overlays:

- `example-development/`
- `example-production/`

These overlays use placeholder registries, domains, namespaces, and secrets.
Replace every `change-me-*`, `registry.example.com`, and `example.com` value
before applying them to a real cluster.
