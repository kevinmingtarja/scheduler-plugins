# Kubernetes scheduler plugins

A collection of Kubernetes scheduler plugins for educational purposes. Each
plugin explores how the scheduling framework can customize pod placement.
The first example is `Maint`, a maintenance-aware placement plugin.

## Plugins

### Maint: maintenance-aware placement

`Maint` rejects nodes with maintenance starting in 30 minutes or less. Among
eligible nodes, it scores time remaining linearly from 0 at 30 minutes to 100
at six hours. Nodes without a maintenance annotation receive 100.

The annotation affects new scheduling attempts. Draining nodes and terminating
running pods for maintenance require a separate controller or administrator.
`EventsToRegister` retries rejected pods on node additions and annotation updates.

## Run the Maint example on kind

Use kind v0.33.0 for the Kubernetes v1.37.0 node image.

```sh
go test ./pkg/maint
docker build -t maint-scheduler:kind .
kind create cluster --name maint-test --image kindest/node:v1.37.0 \
  --config deploy/kind.yaml --kubeconfig /tmp/maint-test.kubeconfig
kind load docker-image maint-scheduler:kind --name maint-test
kubectl --context kind-maint-test --kubeconfig /tmp/maint-test.kubeconfig \
  apply -f deploy/scheduler.yaml
python3 deploy/test-kind.py /tmp/maint-test.kubeconfig
```

Run the live test once the scheduler pod is Running. It creates a separate test
namespace, changes maintenance annotations on the two workers, and leaves its
pods for inspection. It checks placement in both directions, the maintenance
cutoff, stable Pending status, recovery after an annotation update, and preference
for a node without scheduled maintenance.

The demo configuration keeps the default filtering plugins and uses `Maint` as
the only scoring plugin so its placement preferences can be observed directly.
The custom scheduler runs alongside the default scheduler. Select it on a pod:

```yaml
spec:
  schedulerName: maint-scheduler
```

Set the node annotation `scheduling.example.com/maintenance-start` to an RFC3339
timestamp. For example:

```sh
kubectl --context kind-maint-test --kubeconfig /tmp/maint-test.kubeconfig \
  annotate node maint-test-worker \
  scheduling.example.com/maintenance-start=2026-09-20T14:00:00Z --overwrite
```
