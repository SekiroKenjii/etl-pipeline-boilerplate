"""The CRM pipelines, every night, one Kubernetes pod per pipeline.

full_load, then the orphans parked until their parents arrived, then the
deletes, then the cutover counts. Each task runs the ETL's image with
`etl run <pipeline>`; its exit code decides:

- 0: success, partial success included (`--partial-ok`): rows rejected
  only, alerts watch the rejects;
- any other code fails the task, and Airflow retries it: 1 a failed step,
  2 invalid settings, 4 another run holds some of the steps (a manual
  run, say). A locked night fails, and alerts: it is never skipped.

Each run is labelled with its DAG run and task (`--label`), and a retry
resumes it (`--resume`): it runs the steps the failed try did not finish.
The step locks are the try's process's: a retry waits, up to the lease,
for those of the try before it. A try that died (OOM) frees them within
that time; a zombie try still running (its worker lost, its pod alive)
keeps them, and the retry exits 4 rather than run the same steps at once.
The next DAG run is another label: every step runs.

Settings come from the environment of the Airflow workers:

- ETL_IMAGE: the image by digest, as the release prints it, e.g.
  ghcr.io/acme/etl-pipeline:1.2.0@sha256:<digest>: a tag can be moved,
  a digest is the image that was signed and scanned.
- ETL_NAMESPACE: where the pods run.
- ETL_POD_TEMPLATE: the pod (user, secrets, volumes); `etl_pod.yaml` by
  default.
- ETL_SEED_DEMO=1: seeds the demo databases first, for `make airflow-demo`
  only.
"""

from __future__ import annotations

import datetime
import os
import pathlib

from airflow.providers.cncf.kubernetes.operators import pod
from airflow.sdk import DAG

IMAGE = os.environ.get("ETL_IMAGE", "ghcr.io/example/etl-pipeline:0.1.0")
NAMESPACE = os.environ.get("ETL_NAMESPACE", "etl")
POD_TEMPLATE = os.environ.get(
    "ETL_POD_TEMPLATE", str(pathlib.Path(__file__).with_name("etl_pod.yaml"))
)


def etl(task_id: str, *arguments: str) -> pod.KubernetesPodOperator:
    """A task running `etl <arguments>` in a pod of the ETL's image."""
    return pod.KubernetesPodOperator(
        task_id=task_id,
        name=f"etl-{task_id.replace('_', '-')}",
        namespace=NAMESPACE,
        image=IMAGE,
        pod_template_file=POD_TEMPLATE,
        arguments=list(arguments),
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        startup_timeout_seconds=300,
    )


def run(task_id: str, pipeline: str) -> pod.KubernetesPodOperator:
    """A task running `etl run <pipeline>`, labelled with its DAG run and
    task: its retries resume its run."""
    return etl(
        task_id,
        "run",
        pipeline,
        "--partial-ok",
        "--label",
        f"{{{{ run_id }}}}/{task_id}",
        "--resume",
    )


with DAG(
    dag_id="etl_crm",
    description="The CRM migration: load, catch up, reconcile.",
    schedule="0 2 * * *",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        # Longer than the pod's terminationGracePeriodSeconds (300).
        "retry_delay": datetime.timedelta(minutes=6),
    },
    tags=["etl", "crm"],
):
    load = run("full_load", "crm.full_load")
    if os.environ.get("ETL_SEED_DEMO") == "1":
        seed = etl("seed_demo", "run", "--help")
        seed.cmds = ["python", "-m", "modules.crm.demo"]
        seed.arguments = ["--env", "dev"]
        seed >> load  # pylint: disable=pointless-statement
    (
        load  # pylint: disable=expression-not-assigned
        >> run("parked", "crm.parked")
        >> run("deletes", "crm.deletes")
        >> etl("cutover_counts", "cutover", "report", "crm", "--counts-only")
    )
