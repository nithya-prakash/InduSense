# 14. Kubernetes + Helm

[infrastructure/helm/indusense/](../../infrastructure/helm/indusense/) is a
Helm chart that deploys the entire stack — every StatefulSet, Deployment,
and one-time setup Job docker-compose.yml already defines — as a single
release, without changing a line of application code: every service's
`config.py` just reads environment variables and doesn't know or care
whether Compose or Kubernetes set them.

**Verified against a real cluster, start to finish** (numbers below are
from the original Go-backend build — see the note in
[docs/phases/README.md](README.md); the chart itself is unchanged by the
later Python rewrite, since it only references image tags, not language):
Docker Desktop's Kubernetes (kind-based, 1 node), a completely fresh
`helm install --set seed.enabled=true` converging to all 15 pods `Running`
in 66 seconds wall-clock, real demo data landing in Postgres (2 orgs, 6
users, 200 devices, 1000 sensors), a real login returning a genuine JWT
through a `kubectl port-forward`'d API, and — firing the in-cluster
simulator Job — the full pipeline processing real traffic end to end,
visible through the actual API, Grafana, Prometheus, and Jaeger.

**Three real bugs were found and fixed by actually deploying, not by
reading the YAML:**

1. **Kafka wouldn't start: a headless-Service DNS chicken-and-egg.** Kafka's
   own KRaft controller-registration step needs to resolve its advertised
   name during startup, before the broker is Ready — but a headless
   Service excludes not-yet-Ready pods from DNS by default, so the broker
   could never resolve itself and crash-looped forever. Fixed with
   `publishNotReadyAddresses: true` on Kafka's Service.
2. **Kafka's health probes always failed, even once healthy** — the probe
   command boots a fresh JVM on every invocation, routinely over
   Kubernetes' 1-second default probe timeout. Fixed with an explicit
   `timeoutSeconds: 10`.
3. **LoadBalancer Services never got an external IP, and blocked `helm
   uninstall`.** Docker Desktop's Kubernetes has no cloud-provider-kind/
   MetalLB, so `api`/`frontend`'s LoadBalancer Services sat at `<pending>`
   forever, and their cleanup finalizer then blocked namespace deletion.
   Switched the default to `ClusterIP` + `kubectl port-forward`.

**Migrations and topic creation as Helm hooks** run
`post-install,pre-upgrade` — deliberately not `pre-install`, which runs
*before* the release's own Postgres/Kafka StatefulSets exist. Migrations
are baked into a small custom image rather than mounted from a ConfigMap —
a 1MiB size limit and decoupling the schema from the image that applies it
are both real problems a ConfigMap would create.

**Demo data seeding** reuses `scripts/seed` unmodified, containerized. It's
a `post-install,post-upgrade` hook, off by default (`seed.enabled=false`) —
`post-upgrade` matters because `helm upgrade --set seed.enabled=true` is
the documented way to seed after the fact, and post-install-only hooks
never fire on upgrades.

**Traffic generation runs inside the cluster**, unlike Compose's simulator
profile, which runs from the host machine — Kafka's advertised listener
only resolves inside the cluster (see bug #1's fix).

**Not implemented here**: an ingress controller/Ingress resource (written
and gated behind `ingress.enabled=false`); Kafka/Postgres clustering or
multi-replica stateful components; Horizontal Pod Autoscaling;
NetworkPolicies. `api` runs 2 replicas behind its Service as the one
genuinely meaningful horizontal-scaling demonstration.
