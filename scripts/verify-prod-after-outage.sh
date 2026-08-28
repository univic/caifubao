#!/usr/bin/env bash
# ============================================================================
# verify-prod-after-outage.sh — Post-node-outage verification for Caifubao prod
#
# Run after vm-4-12 (or any MongoDB-hosting node) returns to Ready:
#   1. Node + MongoDB + backend readiness
#   2. prod datahub can reach MongoDB
#   3. Data chain freshness (quote/factor/index/signal/score latest dates)
#   4. prod health-watcher baseline (no --fail-on-issues)
#   5. dev data-sync catch-up (prod → dev)
#
# Env:
#   KUBECONFIG   Path to k3s.yaml (default ~/.kube/config)
#   CFB_NAMESPACE Prod namespace (default caifubao)
# ============================================================================
set -uo pipefail

KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
CFB_NS="${CFB_NAMESPACE:-caifubao}"
K() { KUBECONFIG="$KUBECONFIG" kubectl "$@"; }

echo "=== 1. Node / MongoDB / backend readiness ==="
K get node vm-4-12-ubuntu -o jsonpath='node ready={.status.conditions[?(@.type=="Ready")].status}{"\n"}' 2>&1
K -n "$CFB_NS" get pod mongodb-0 -o jsonpath='mongodb ready={.status.conditions[?(@.type=="Ready")].status}{"\n"}' 2>&1
K -n "$CFB_NS" get pods -l app=caifubao-backend 2>&1 | tail -2

echo ""
echo "=== 2. prod datahub → MongoDB connectivity ==="
K -n "$CFB_NS" exec deploy/caifubao-datahub -- python3 -c "
from pymongo import MongoClient
import os
c = MongoClient(host=os.environ['MONGODB_HOST'], port=int(os.environ['MONGODB_PORT']),
    username=os.environ['MONGODB_USER'], password=os.environ['MONGODB_PASS'],
    authSource='admin', serverSelectionTimeoutMS=8000)
c.admin.command('ping')
print('MongoDB reachable from datahub')
" 2>&1 | tail -1

echo ""
echo "=== 3. Data chain freshness ==="
K -n "$CFB_NS" exec deploy/caifubao-datahub -- python3 -c "
from pymongo import MongoClient
import os
c = MongoClient(host=os.environ['MONGODB_HOST'], port=int(os.environ['MONGODB_PORT']),
    username=os.environ['MONGODB_USER'], password=os.environ['MONGODB_PASS'],
    authSource='admin', serverSelectionTimeoutMS=8000, socketTimeoutMS=15000)
db = c[os.environ['MONGODB_NAME']]
for code in ['sz000977', 'sh600000']:
    q = db.stock_daily_quote.find_one({'code': code}, {'date': 1}, sort=[('date', -1)])
    f = db.stock_factor_daily.find_one({'stock_code': code}, {'date': 1}, sort=[('date', -1)])
    print(f'{code}: quote={str(q[\"date\"])[:10] if q else \"NONE\"} factor={str(f[\"date\"])[:10] if f else \"NONE\"}')
idx = db.data_asset_status.find_one({'asset_type': 'quote', 'object_type': 'stock_index'}, {'latest_data_date': 1})
print('index latest:', str(idx.get('latest_data_date'))[:10] if idx else 'NONE')
s = db.stock_signal_daily.find_one({}, {'date': 1}, sort=[('date', -1)])
sc = db.stock_score_predictions.find_one({}, {'date': 1}, sort=[('date', -1)])
print('signal:', str(s['date'])[:10] if s else 'NONE', '| score:', str(sc['date'])[:10] if sc else 'NONE')
" 2>&1 | head -6

echo ""
echo "=== 4. Health-watcher baseline (no --fail-on-issues) ==="
K -n "$CFB_NS" exec deploy/caifubao-datahub -- python3 -m app.jobs.health_watcher --hours 26 2>&1 | grep -E '"healthy"|no_data_count|stale_count|"failed"' | head -5 || echo "(watcher not in deployed image yet)"

echo ""
echo "=== 5. dev data-sync catch-up ==="
echo "Run manually when ready:"
echo "  KUBECONFIG=\$KUBECONFIG kubectl -n caifubao-dev exec deploy/caifubao-datahub -- python3 -m app.jobs.data_sync_runner run"
