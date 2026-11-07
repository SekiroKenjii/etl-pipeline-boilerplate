# Deployment

The ETL ships as one image, which runs `etl`. Schedulers start it, one run
of one pipeline at a time. This page shows the two usual ways: Kubernetes
CronJobs, and an Airflow DAG whose tasks are Kubernetes pods. CI checks both
on a real cluster (kind) at every push.

## The image

```sh
make image                       # localhost/etl-pipeline:dev
docker run --rm localhost/etl-pipeline:dev --env dev list
```

What is in the image:

- **Contents.** It is built from `uv.lock`, with every connector by
  default; `--build-arg EXTRAS="--extra app --extra postgres"` builds a
  smaller one. It holds the package, `config/` and `mappings/`.
- **User.** It runs as user 10001, and works with a read-only root
  filesystem when `/app/var` and `/tmp` are writable volumes.
- **Overrides.** Mounted files and `ETL__*` variables override the settings.
  Secrets are files (`${file:...}`) or come from a secret manager (see
  `docs/security.md`).
- **Base images.** `python` and `uv` are pinned by digest in the
  `Dockerfile`: a build takes the same bytes until a reviewed commit changes
  the digest (Dependabot proposes it weekly). OS patches come that way, not
  from `apt-get upgrade` in the build: the official Python image is rebuilt
  on each update of its Debian base. pip is removed from the final image.
- **Scanning.** `make image-scan` (trivy) fails on a high or critical
  vulnerability that has a fix, in OS packages and Python libraries alike;
  CI runs it at every push and every week, and the release before it
  pushes. When it fails, take the new base digest, or upgrade the library in
  `uv.lock`; an ignore goes in `.trivyignore` with its reason.
  `make image-sbom` writes the image's bill of materials, OS packages
  included (syft, CycloneDX), to `var/image.cdx.json`.
- **Releases.** A tag `v<version>` publishes
  `ghcr.io/<owner>/<repo>:<version>` (`.github/workflows/release.yml`), and
  no `latest`. The release signs the image (cosign, keyless: the identity is
  the release workflow), attests its build provenance (SLSA) and its SBOM,
  and prints its digest in the release notes.
- **What a release checks.** `make check` (lint, types, the tests on
  SQLite only), `make security` and the image scan. Not the other
  databases, the coverage floor, the documentation, the collector
  configurations or the Kubernetes and Airflow demos: CI runs those on
  every push. Tag only a commit whose CI run passed.

### Deploying by digest

A tag can be pushed again; a digest cannot. Deploy the digest the release
prints, after checking its signature:

```sh
cosign verify ghcr.io/<owner>/<repo>@sha256:<digest> \
  --certificate-identity-regexp \
    '^https://github.com/<owner>/<repo>/.github/workflows/release.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/<owner>/<repo>@sha256:<digest> \
  --repo <owner>/<repo>
cd deploy/k8s/base
kustomize edit set image \
  etl-pipeline=ghcr.io/<owner>/<repo>:<version>@sha256:<digest>
```

For Airflow, `ETL_IMAGE` takes the same `<image>@sha256:<digest>`. An
admission policy (Sigstore policy-controller, Kyverno `verifyImages`) can
refuse unsigned images in the namespace.

## What a scheduler acts on

`etl run <module.pipeline>` exits with these codes (every command's are in
{doc}`reference/cli`):

| Code | Meaning | Kubernetes Job | Airflow task |
| --- | --- | --- | --- |
| 0 | Success. A partial success too, with `--partial-ok` | Complete | success |
| 1 | Failed: a step failed | Retried (`backoffLimit`) | Retried (`retries`) |
| 2 | Invalid settings or arguments; a pipeline that cannot be built; keys changed without `--rekey` | Fails at once (`podFailurePolicy`) | Failed after its retries |
| 3 | Partial success, without `--partial-ok` | Failed | Failed |
| 4 | Another run holds some of the steps | Fails at once | Retried, then failed |
| 130 | Cancelled (SIGTERM, SIGINT) | Not counted on a node drain | Failed |

- **Partial success.** A partial success means rows were rejected into the
  quarantine, and nothing else: a failed step is exit 1, even one the
  pipeline went on without. With `--partial-ok` the run counts as done: the
  `EtlRejectRateHigh` alert and `etl quarantine list` deal with the rejects,
  and the run ledger still records `partial_success`.
- **Cancellation.** On SIGTERM, each running step finishes its batch, and
  the run ends as cancelled. Leave time for one batch:
  `terminationGracePeriodSeconds` is 300.
- **Locks.** Two runs never run the same step at once, whatever the
  scheduler: each step has a lock in the database, `<pipeline>/<step>`, with
  a lease. A run locked out exits 4 and fails: a locked night alerts, it is
  never skipped. A pod that dies loses its leases after `lease_seconds`, and
  the next run takes over.
- **Retries.** The CronJobs run `etl run ... --label $(JOB_NAME) --resume`:
  the run is labelled with its Job. A pod retried by the Job resumes that
  run, runs only the steps it did not finish, and takes over the locks of
  the pod that died; the next Job is a new label, which runs every step.
  The DAG does the same with a label per DAG run and task.

## Kubernetes

`deploy/k8s/base` holds:

- one CronJob per pipeline, `crm-full-load`, and `etl-housekeeping` for the
  retention of the quarantine;
- a service account without API access;
- a NetworkPolicy of their pods (`networkpolicy.yaml`).

It needs Kubernetes 1.27 or later: the CronJob reads its Job's name from
the `batch.kubernetes.io/job-name` label, and uses `podFailurePolicy` and
`podReplacementPolicy`.

Every pod runs as non-root, with a read-only root filesystem, no
capabilities and the default seccomp profile.

```sh
kubectl create secret generic etl-secrets \
  --from-file=legacy-database-url=legacy.url \
  --from-file=crm-database-url=crm.url
kubectl apply -k deploy/k8s/base
kubectl create job --from=cronjob/crm-full-load crm-full-load-now
```

### Hardening

- **Secrets are files.** The connection strings are mounted from
  `etl-secrets` under `/run/secrets` and read by `${file:...}` in
  `config/prod.toml`, never passed as environment variables: those show in
  `/proc/<pid>/environ` and are inherited by every process the ETL starts.
  `--from-file` keeps them out of the shell history too. The Secret is
  required: a missing Secret or key keeps the pod pending, which shows at
  once, instead of an exit 2 in the night.
- **Image.** `imagePullPolicy: Always`, with the image named by digest
  (above): the node checks the pod may pull it, and nothing can swap it.
- **Network.** `networkpolicy.yaml` denies all ingress, and all egress but
  DNS, the databases and the OpenTelemetry Collector. Its addresses and
  ports are placeholders: put your databases' addresses (pod or endpoint
  addresses: a policy sees them after a Service is resolved), and add a rule
  for each other service the settings use (a secret manager, Kafka, a cloud
  identity endpoint). It is enforced only by a network plugin that
  implements NetworkPolicy (Calico, Cilium; kindnet on kind).
- **Deadlines.** The full load stops after 8 hours, housekeeping after one
  (`activeDeadlineSeconds`); exit 2 (invalid settings) fails either Job
  without retries.
- **Telemetry.** The collector configurations of `deploy/otel-collector/`
  accept OTLP over TLS only, with a bearer token, on the pod's address; the
  ETL sends to `https://...` with `Authorization: Bearer
  ${file:/run/secrets/otlp_token}` (`logging.otlp` in `config/prod.toml`).
  Mutual TLS is an option there.

Add a pipeline by copying the CronJob and changing its name, schedule and
`args`. Set the image in `kustomization.yaml`.

### Several pods for one pipeline

A run is one process; the scheduler shares a large load out, each job its
own steps ({doc}`operations`). With `partitions = 4` of the CRM module, for
instance:

1. a Job running `--step '*:ranges'`, which splits the tables once;
2. an Indexed Job of 4 pods, each running the ranges of its index:
   `--step 'customer:$(JOB_COMPLETION_INDEX)' --label
   $(JOB_NAME)-$(JOB_COMPLETION_INDEX) --resume`, with
   `backoffLimitPerIndex` so that each index is retried on its own;
3. the same for the child tables (`order:...`), once their parents loaded.

Airflow expresses it as tasks: one per range, the ranges task first. Each
job needs a label of its own, and the bookkeeping in a database every pod
reaches, as in `config/prod.toml`.

To try it locally, `make kind-up k8s-demo` runs the demo on kind: an init
container seeds SQLite, then `crm.full_load` runs. With rootless podman,
kind needs the cgroup controllers delegated; the Makefile runs it in a
`systemd-run --user -p Delegate=yes` scope. `make kind-down` deletes the
cluster.

## Airflow

`deploy/airflow/dags/etl_crm.py` is a DAG of four tasks:

1. `full_load`;
2. `parked`;
3. `deletes`;
4. `cutover_counts`.

Each task is a `KubernetesPodOperator` running the image. The pod comes from
`etl_pod.yaml`, with the same security as the CronJobs: the secrets as
files, the label the NetworkPolicy selects (`app.kubernetes.io/name:
etl`), `imagePullPolicy: Always`. The DAG reads:

- `ETL_IMAGE`: the image to run, by digest.
- `ETL_NAMESPACE`: where the pods run (`etl` by default).
- `ETL_POD_TEMPLATE`: another pod template.

It needs the `apache-airflow-providers-cncf-kubernetes` provider, which the
official Airflow image includes, and a connection to the cluster: in
cluster, or a kubeconfig.

`make airflow-demo` runs the DAG with `airflow dags test`, in the official
Airflow image, against kind. A seed task comes first (`ETL_SEED_DEMO=1`),
and the tasks share SQLite on a volume.

Each `etl run` task carries `--label {{ run_id }}/<task> --resume`: a
retry resumes the run of its try, and takes over the locks of a try that
died. Exit 4 (another run holds some of the steps) fails the task, which is
retried, then fails the DAG run: a locked night is never marked a success.

Without Kubernetes, a `BashOperator` running `etl run crm.full_load
--partial-ok --label {{ run_id }} --resume` on workers that have the package
works too.
