#!/bin/bash
set -e

# Create local minikube cluster
minikube start \
  --cpus=18 \
  --memory=20384 \
  --disk-size=100g \
  --addons=dashboard,metrics-server \
  --container-runtime=docker \
  --kubernetes-version=v1.34.0

echo "Now we sleep for 10 seconds and wait for a stable minikube setup"
sleep 10

# Install DAPR CLI on local machine & control plane on minikube
if ! command -v dapr &> /dev/null; then
    wget -q https://raw.githubusercontent.com/dapr/cli/master/install/install.sh -O - | /bin/bash
fi

CURRENT_CTX=$(kubectl config current-context)
if [[ "$CURRENT_CTX" != "minikube" ]]; then
    echo "Warning: Your current context is $CURRENT_CTX, not minikube"
fi

dapr init -k --wait || true
echo "Now we sleep for 10s and wait for DAPR to stabilise"
sleep 10
dapr status -k

# Now lets install a local & minimal pulsar cluster for testing
# kubectl apply -f pulsar_final.yaml -n pulsar-mini

rm -rf ./pulsar-helm-chart/ || true
git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
./scripts/pulsar/prepare_helm_release.sh \
    -n pulsar \
    -k pulsar-mini \
    -c

kubectl delete validatingwebhookconfigurations -l app.kubernetes.io/instance=pulsar-mini 2>/dev/null || true
kubectl delete mutatingwebhookconfigurations -l app.kubernetes.io/instance=pulsar-mini 2>/dev/null || true

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus-operator prometheus-community/prometheus-operator-crds

echo "📦 Installing Pulsar (Lean Mode)..."
helm upgrade --install pulsar-mini apache/pulsar \
    --namespace pulsar \
    --values examples/values-minikube.yaml \
    --set extra.monitoring.enabled=false \
    --set victoria-metrics-operator.enabled=false \
    --set victoria-metrics-k8s-stack.enabled=false \
    --set broker.resources.requests.cpu=2 \
    --set broker.resources.limits.cpu=3 \
    --set bookie.resources.requests.cpu=2 \
    --set bookie.resources.limits.cpu=3 \
    --set bookie.configData.journalSyncData="false" \
    --set bookie.configData.journalAdaptiveGroupWrites="true"

echo "Now we sleep for 600 seconds and allow pulsar to stabilise"
sleep 600
echo "WARN: 4-5 broker & proy pod restarts are normal & expected"
echo "WARN: 600s seconds is not often enough for pulsar to stabilise, you may have to wait more"
sleep 5
kubectl get po -A -w

# ./bin/pulsar-admin topics create-partitioned-topic persistent://public/default/hammer-topic --partitions 16

# ./bin/pulsar-admin topics set-message-ttl persistent://public/default/hammer-topic --ttl 5

# ./bin/pulsar-admin topics set-backlog-quota persistent://public/default/hammer-topic --limit 5G --policy consumer_backlog_eviction

echo "Now, go do your helm thing and run pulsar-hammer application"