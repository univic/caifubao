## ADDED Requirements

### Requirement: Object Storage Backup Boundary

Caifubao SHALL support MongoDB backup artifacts in an S3-compatible object
storage target before the next cluster initialization is treated as complete.

#### Scenario: Scheduled MongoDB backup completes

- **GIVEN** the runtime has MongoDB credentials and object-storage credentials
  from private deployment configuration
- **WHEN** the backup job runs
- **THEN** it SHALL create a compressed logical MongoDB dump artifact
- **AND** upload the artifact to an S3-compatible bucket or prefix
- **AND** record backup start time, finish time, object key, database name,
  namespace, status, and error summary without logging secret values.

#### Scenario: Public repository defines backup shape

- **GIVEN** backup support is added to the public caifubao repository
- **WHEN** manifests, docs, or examples are committed
- **THEN** they SHALL contain only placeholder endpoint, bucket, access-key,
  secret-key, and retention values
- **AND** real object-storage targets, private domains, credentials, and
  operator runbooks SHALL remain outside the public repository.

### Requirement: Restore Drill

Caifubao SHALL define a repeatable restore path from object storage to a fresh
MongoDB instance.

#### Scenario: Operator restores into a fresh cluster

- **GIVEN** a fresh MongoDB instance is running with an empty caifubao database
- **WHEN** an operator launches the restore job with an approved backup object
- **THEN** the job SHALL download the backup artifact from object storage
- **AND** restore the configured database into MongoDB
- **AND** run post-restore sanity checks for required collections, document
  counts, and freshness metadata.

#### Scenario: Restore is validated before application traffic

- **GIVEN** restore has completed
- **WHEN** backend and datahub are about to be enabled
- **THEN** the system SHALL verify health endpoints, data quality summary,
  quote freshness, and scoring/factor collection presence
- **AND** fail closed if required collections or freshness markers are missing.

### Requirement: Cluster Reinitialization Bootstrap

Caifubao SHALL provide an explicit empty-database bootstrap path for cases where
no restorable backup exists.

#### Scenario: No backup is available

- **GIVEN** MongoDB data is unrecoverable
- **WHEN** the cluster is reinitialized
- **THEN** operators SHALL be able to initialize stock master data, quote data,
  factors, signals, scores, job status, and data quality metadata in a documented
  order
- **AND** the bootstrap path SHALL distinguish regenerable market data from
  non-regenerable user, portfolio, service-token, audit, and decision-journal
  data.

#### Scenario: Bootstrap reaches demo-ready state

- **GIVEN** the empty-database bootstrap has completed
- **WHEN** validation runs
- **THEN** the system SHALL confirm that backend APIs, frontend dashboards,
  OpenClaw read endpoints, and datahub freshness checks can operate against the
  regenerated dataset
- **AND** document any intentionally missing non-regenerable data.

#### Scenario: Interrupted quote bootstrap resumes safely

- **GIVEN** an empty-database quote bootstrap crosses midnight or its Pod restarts
- **WHEN** the same logical bootstrap attempt resumes
- **THEN** it SHALL retain the original frozen target trading date
- **AND** an operator-initiated retry SHALL pass the original target through the explicit quote-runner cutoff option
- **AND** scheduled quote Jobs SHALL fail visibly instead of automatically retrying with a newly calculated cutoff
- **AND** safely replay completed stocks and fill incomplete stocks
- **AND** recalculate quote freshness and coverage against the frozen target date
- **AND** factor, signal, scoring, and Parquet phases SHALL remain disabled until quote validation completes
- **AND** a new independent bootstrap MAY calculate a new target date as a new observable run.

### Requirement: Persistent Storage Hardening

Caifubao SHALL not rely on an unqualified single-node local-path PVC as the only
durability mechanism for MongoDB in long-lived environments.

#### Scenario: MongoDB storage is provisioned

- **GIVEN** MongoDB is deployed in a long-lived development or production-like
  cluster
- **WHEN** its persistent storage is configured
- **THEN** the storage plan SHALL specify the persistence class, reclaim policy,
  node placement assumptions, backup dependency, and restore procedure
- **AND** it SHOULD prefer a replicated or snapshot-capable storage backend
  where available.

#### Scenario: local-path is used temporarily

- **GIVEN** local-path storage is used for a temporary environment
- **WHEN** MongoDB is initialized
- **THEN** the deployment SHALL document that the PVC is not a disaster-recovery
  boundary
- **AND** scheduled object-storage backups SHALL be enabled before meaningful
  non-regenerable data is created.

#### Scenario: static local storage is used for a long-lived single-node workload

- **GIVEN** replicated or snapshot-capable storage is not available
- **WHEN** MongoDB uses a static local persistent volume
- **THEN** the PV SHALL use the `Retain` reclaim policy and identify an explicit
  host path and node affinity
- **AND** the MongoDB workload SHALL be constrained to the node that owns that
  path
- **AND** development and production SHALL use separate volumes and paths
- **AND** operators SHALL treat COS logical backups, rather than the local PV,
  as the node-loss recovery boundary.

### Requirement: Autoresearch Implementation Readiness

Autoresearch scaffolding SHALL start only after the repository has a documented
baseline dataset or explicit empty-database bootstrap path.

#### Scenario: Autoresearch bootstrap is prepared

- **GIVEN** the cluster has either restored data or regenerated demo-ready data
- **WHEN** the autoresearch adapter is implemented
- **THEN** the implementation SHALL create the profile, state, results, ledger,
  metric extraction, and readiness checks described by the approved
  autoresearch plan
- **AND** it SHALL use only the approved research edit scope.

#### Scenario: Storage recovery work is still in progress

- **GIVEN** backup, restore, or bootstrap validation is incomplete
- **WHEN** autoresearch work is considered
- **THEN** implementation SHALL be limited to docs, profiles, adapters, and
  synthetic metric extraction tests
- **AND** no experiment result SHALL be promoted as production evidence.

### Requirement: Tailnet-Only Deployment Access

Caifubao SHALL support GitHub Actions deployment to a cluster whose Kubernetes
API server is reachable only inside the Tailscale tailnet.

#### Scenario: Private deploy workflow connects through Tailscale

- **GIVEN** the public repository has published an image and dispatched a
  private deployment workflow
- **WHEN** the private workflow starts
- **THEN** it SHALL join the tailnet as a tagged CI node
- **AND** configure kubectl through the Tailscale Kubernetes Operator API server
  proxy
- **AND** apply private deployment overlays without exposing kube-apiserver to
  the public internet.

#### Scenario: API server proxy uses Kubernetes RBAC

- **GIVEN** the API server proxy runs in auth mode
- **WHEN** a tagged GitHub Actions runner connects through the proxy
- **THEN** Kubernetes SHALL authorize it through RBAC bound to the impersonated
  tailnet tag group
- **AND** public examples SHALL avoid real tailnet names, OAuth credentials,
  private namespaces, or production ACL files.

#### Scenario: In-cluster proxy is unavailable

- **GIVEN** the cluster cannot schedule the Tailscale proxy pods
- **WHEN** normal API server proxy deployment fails
- **THEN** operators SHALL use an explicitly documented break-glass path such as
  Tailscale SSH to a control-plane node or a temporary tailnet-IP kubeconfig
- **AND** that break-glass path SHALL remain outside public deployment examples.
