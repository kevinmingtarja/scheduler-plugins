"""Exercise the deployed scheduler on the maint-test kind cluster."""

import datetime
import json
import subprocess
import sys
import time


KUBECTL = [
    "kubectl", "--context", "kind-maint-test", "--kubeconfig",
    sys.argv[1] if len(sys.argv) > 1 else "/tmp/maint-test.kubeconfig",
    "--request-timeout=15s",
]
NAMESPACE = "maint-e2e-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
ANNOTATION = "scheduling.example.com/maintenance-start"
WORKER = "maint-test-worker"
WORKER2 = "maint-test-worker2"


def kubectl(*args, obj=None):
    return subprocess.check_output(
        KUBECTL + list(args), input=json.dumps(obj) if obj else None, text=True,
    )


def maintenance(node, minutes):
    if minutes is None:
        value = ANNOTATION + "-"
    else:
        deadline = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
        value = ANNOTATION + "=" + deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    kubectl("annotate", "node", node, value, "--overwrite")


def create_pod(name, node=None):
    selector = {"kubernetes.io/hostname": node} if node else {"maint-test": "true"}
    kubectl("create", "-f", "-", obj={
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "schedulerName": "maint-scheduler", "nodeSelector": selector,
            "containers": [{
                "name": "pause", "image": "registry.k8s.io/pause:3.10",
                "imagePullPolicy": "Never",
                "resources": {"requests": {"cpu": "10m", "memory": "8Mi"}},
            }],
        },
    })


def get_pod(name):
    pod = json.loads(kubectl("get", "pod", name, "-n", NAMESPACE, "-o", "json"))
    if pod["status"]["phase"] in ("Failed", "Succeeded"):
        raise RuntimeError(pod["status"])
    for container in pod["status"].get("containerStatuses", []):
        state = container["state"]
        reason = state.get("waiting", {}).get("reason")
        if "terminated" in state or reason in (
            "CrashLoopBackOff", "ErrImagePull", "ImagePullBackOff",
            "ErrImageNeverPull", "CreateContainerConfigError", "RunContainerError",
        ):
            raise RuntimeError(state)
    return pod


def await_pod(name, expected_node=None):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        pod = get_pod(name)
        if expected_node:
            assigned = pod["spec"].get("nodeName")
            if assigned and assigned != expected_node:
                raise AssertionError(f"{name}: expected {expected_node}, got {assigned}")
            if assigned and pod["status"]["phase"] == "Running":
                print(f"PASS {name}: Running on {assigned}", flush=True)
                return pod
        else:
            if pod["spec"].get("nodeName"):
                raise AssertionError(f"{name} escaped maintenance filter")
            for condition in pod["status"].get("conditions", []):
                if (condition["type"] == "PodScheduled" and condition["status"] == "False"
                        and "node will start maintenance" in condition.get("message", "")):
                    print(f"PASS {name}: rejected by maintenance filter", flush=True)
                    return pod
        time.sleep(1)
    raise TimeoutError(f"{name}: {pod['status']}")


kubectl("create", "namespace", NAMESPACE)
print(f"Test namespace: {NAMESPACE}", flush=True)
kubectl("label", "nodes", WORKER, WORKER2, "maint-test=true", "--overwrite")

maintenance(WORKER, 60)
maintenance(WORKER2, 300)
create_pod("prefer-later")
await_pod("prefer-later", WORKER2)

maintenance(WORKER, 300)
maintenance(WORKER2, 60)
create_pod("prefer-later-swapped")
await_pod("prefer-later-swapped", WORKER)

maintenance(WORKER, 10)
create_pod("avoid-cutoff")
await_pod("avoid-cutoff", WORKER2)
create_pod("blocked-cutoff", WORKER)
blocked = await_pod("blocked-cutoff")
time.sleep(5)
still_blocked = get_pod("blocked-cutoff")
assert blocked["metadata"]["resourceVersion"] == still_blocked["metadata"]["resourceVersion"], (
    "Blocked pod status kept changing without a maintenance update"
)
print("PASS blocked-cutoff: status stable for five seconds", flush=True)

maintenance(WORKER, 300)
recovered = await_pod("blocked-cutoff", WORKER)
assert recovered["metadata"]["uid"] == blocked["metadata"]["uid"]
print("PASS annotation update: same pod recovered", flush=True)

maintenance(WORKER, None)
create_pod("prefer-no-maintenance")
await_pod("prefer-no-maintenance", WORKER)
print("All live scheduling checks passed. Pods and annotations left for inspection.", flush=True)
