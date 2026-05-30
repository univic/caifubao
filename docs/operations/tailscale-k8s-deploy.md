# Tailscale Kubernetes API Server Deploy Path

Caifubao deployment should use the Tailscale Kubernetes Operator API server
proxy when the K3S cluster is reachable only inside the tailnet.

The public repository provides templates only. Real tailnet names, OAuth
credentials, private namespaces, ACL ownership, and deployment overlays belong
in `caifubao-private`.

## Architecture

```text
public repo publish workflow
        |
        | repository_dispatch
        v
private repo deploy workflow
        |
        | tailscale/github-action joins tailnet as tag:ci-deploy
        v
Tailscale API server proxy (ProxyGroup tag:k8s)
        |
        v
K3S kube-apiserver
```

The public repository should continue to publish images and dispatch private
deploys. The private repository owns the actual tailnet connection and
`kubectl apply`.

## Cluster Template

The recommended public template is:

- `k8s/tailscale/api-server-proxy.proxygroup.example.yaml`

It creates a high-availability `ProxyGroup` of type `kube-apiserver`:

```text
caifubao-kubeapi.<tailnet>.ts.net
```

Private overlays should set the final hostname, tags, replica count, and
operator install values.

The RBAC example is:

- `k8s/tailscale/api-server-proxy-rbac.example.yaml`

It binds the tailnet device group `tag:ci-deploy` to the Kubernetes `admin`
ClusterRole inside the example namespace. Private overlays should narrow this
role if deployment does not need full namespace admin privileges.

## Tailnet Policy Shape

Use real values only in the Tailscale admin console or private policy files.
The public shape is:

```json
{
  "tagOwners": {
    "tag:k8s-operator": ["autogroup:admin"],
    "tag:k8s": ["tag:k8s-operator"],
    "tag:ci-deploy": ["autogroup:admin"]
  },
  "autoApprovers": {
    "services": {
      "tag:k8s": ["tag:k8s"]
    }
  },
  "grants": [
    {
      "src": ["tag:ci-deploy"],
      "dst": ["tag:k8s"],
      "ip": ["tcp:80", "tcp:443"]
    }
  ]
}
```

The `tag:ci-deploy` runner is authenticated to Kubernetes as the group
`tag:ci-deploy` when using the API server proxy in auth mode. Kubernetes RBAC
then decides what it can do.

## Operator Install

Install the Tailscale Kubernetes Operator from the private deployment path.
For a `ProxyGroup` API server proxy in auth mode, the operator needs
impersonation support:

```bash
helm repo add tailscale https://pkgs.tailscale.com/helmcharts
helm repo update
helm upgrade --install tailscale-operator tailscale/tailscale-operator \
  --namespace tailscale \
  --create-namespace \
  --set-string oauth.clientId="$TS_OPERATOR_OAUTH_CLIENT_ID" \
  --set-string oauth.clientSecret="$TS_OPERATOR_OAUTH_SECRET" \
  --set-string apiServerProxyConfig.allowImpersonation="true" \
  --wait
```

Then apply the private equivalent of:

```bash
kubectl apply -f k8s/tailscale/api-server-proxy.proxygroup.example.yaml
kubectl wait proxygroup caifubao-kubeapi --for=condition=ProxyGroupReady=true
```

## GitHub Actions Deploy Flow

Private deploy workflows should:

1. Join the tailnet with `tailscale/github-action@v3`.
2. Run `tailscale configure kubeconfig` against the ProxyGroup URL.
3. Apply the private overlay.
4. Wait for rollout.

Example:

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    steps:
    - uses: actions/checkout@v4

    - name: Connect to tailnet
      uses: tailscale/github-action@v3
      with:
        oauth-client-id: ${{ secrets.TS_CI_OAUTH_CLIENT_ID }}
        oauth-secret: ${{ secrets.TS_CI_OAUTH_SECRET }}
        tags: tag:ci-deploy

    - name: Configure kubeconfig through API server proxy
      run: tailscale configure kubeconfig "${{ secrets.TS_KUBEAPI_PROXY_URL }}"

    - name: Deploy private overlay
      run: |
        kubectl apply -k "k8s/overlays/${{ inputs.environment }}"
        kubectl -n "${{ secrets.CFB_NAMESPACE }}" rollout status deploy/caifubao-backend
        kubectl -n "${{ secrets.CFB_NAMESPACE }}" rollout status deploy/caifubao-datahub
        kubectl -n "${{ secrets.CFB_NAMESPACE }}" rollout status deploy/caifubao-frontend
```

`TS_KUBEAPI_PROXY_URL` should look like:

```text
https://caifubao-kubeapi.<tailnet>.ts.net
```

## Fallbacks

Keep one break-glass path outside the in-cluster proxy. The API server proxy
runs inside the cluster, so a cluster that cannot schedule pods can also lose
the proxy. Acceptable break-glass paths include:

- SSH to a control-plane node over Tailscale.
- A temporary kubeconfig that targets the control-plane Tailscale IP.
- Provider console access.

The normal deploy path should still be the API server proxy.
