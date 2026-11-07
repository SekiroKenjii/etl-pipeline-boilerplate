# Day-to-day commands. Everything runs through uv, with the locked versions.
.PHONY: install format lint typecheck test test-db cov cov-db check security docs mappings demo bench image image-tar image-scan image-sbom kind-up kind-load k8s-demo airflow-demo kind-down db-up db-wait db-down otel-validate clean

SOURCES := src tests scripts benchmarks
# Throwaway databases for the integration tests, with podman or docker.
CONTAINER ?= $(shell command -v podman || command -v docker)
MSSQL_PASSWORD := Etl-Passw0rd!
DB_URLS := \
	ETL_TEST_POSTGRES_URL='postgresql+psycopg://etl:etl@localhost:55432/etl' \
	ETL_TEST_MSSQL_URL='mssql+mssqlpython://sa:$(MSSQL_PASSWORD)@localhost:51433/master?TrustServerCertificate=yes&Encrypt=no' \
	ETL_TEST_ORACLE_URL='oracle+oracledb://etl:etl@localhost:51521/?service_name=FREEPDB1' \
	ETL_TEST_ORACLE_ADMIN_URL='oracle+oracledb://system:etl@localhost:51521/?service_name=FREEPDB1' \
	ETL_TEST_MYSQL_URL='mysql+pymysql://etl:etl@127.0.0.1:53306/etl' \
	ETL_TEST_MONGODB_URL='mongodb://localhost:57017/etl?replicaSet=rs0&directConnection=true' \
	ETL_TEST_SEQ_URL='http://localhost:55341' \
	ETL_TEST_KAFKA_URL='localhost:59092' \
	ETL_TEST_OTEL_URL='http://localhost:54318' \
	ETL_TEST_OTEL_DIR='$(CURDIR)/var/otel-test' \
	ETL_TEST_VAULT_URL='http://localhost:58200'

install:  ## Create .venv with every extra and the dev tools, and the git hooks.
	uv sync --all-extras
	uv run pre-commit install

format:  ## Rewrite the code in the project style.
	uv run isort $(SOURCES)
	uv run pyink $(SOURCES)

# Tests, scripts and benchmarks are linted too, less strictly: test names
# say what they check, fixtures shadow and take unused arguments, and
# `(only,) = found` unpacks what pylint cannot size.
TESTS_PYLINT := --disable=missing-docstring,redefined-outer-name \
	--disable=unused-argument,protected-access,duplicate-code \
	--disable=too-many-locals,use-implicit-booleaness-not-comparison \
	--disable=unbalanced-tuple-unpacking

lint:  ## Check the style without changing anything.
	uv run isort --check-only $(SOURCES)
	uv run pyink --check $(SOURCES)
	uv run pylint src
	uv run pylint $(TESTS_PYLINT) tests scripts benchmarks

typecheck:
	uv run mypy
	uv run pyright

# The unit tests run in parallel (pytest-xdist, XDIST=); the others share
# databases, files and timings, and run one at a time. The end-to-end tests
# (`slow`: they start `etl` processes) run last; `-m "not slow"` leaves them
# out of a quick run.
XDIST ?= -n auto
test:  ## The tests; the database ones on SQLite only.
	uv run pytest $(XDIST) tests/units
	uv run pytest -m "not slow" tests/integrations tests/e2e
	uv run pytest -m slow tests/integrations tests/e2e

test-db:  ## The tests on every database (`make db-up` first).
	uv run pytest $(XDIST) tests/units
	$(DB_URLS) uv run pytest -m "not slow" tests/integrations tests/e2e
	$(DB_URLS) uv run pytest -m slow tests/integrations tests/e2e

# Coverage counts the `etl` processes of the end-to-end tests too
# ([tool.coverage.run] patch): each writes a .coverage.* file, which
# pytest-cov combines. Files an earlier, interrupted run left are removed
# first, so that they do not count. Without the databases a good part of the
# adapters is not run, so `cov` reports and does not judge: the floor
# applies where it is reachable, with every database (`cov-db`, as CI).
COV_FLOOR ?= 90
COV_REPORTS := --cov-report=term-missing --cov-report=xml:var/coverage.xml

cov:  ## Coverage of the tests on SQLite (no floor).
	rm -f .coverage .coverage.*
	uv run pytest $(XDIST) tests/units --cov --cov-report=
	uv run pytest -m "not slow" tests/integrations tests/e2e --cov \
		--cov-append --cov-report=
	uv run pytest -m slow tests/integrations tests/e2e --cov --cov-append \
		$(COV_REPORTS)

cov-db:  ## Coverage on every database, failing below COV_FLOOR (CI's gate).
	rm -f .coverage .coverage.*
	uv run pytest $(XDIST) tests/units --cov --cov-report=
	$(DB_URLS) uv run pytest -m "not slow" tests/integrations tests/e2e \
		--cov --cov-append --cov-report=
	$(DB_URLS) uv run pytest -m slow tests/integrations tests/e2e --cov \
		--cov-append $(COV_REPORTS) --cov-fail-under=$(COV_FLOOR)

# The CRM module's transforms (crm.phone...), which its workbook may name.
MAPPINGS_REGISTRY := modules.crm.transforms:registry

mappings:  ## Check that mappings/compiled/ matches the workbooks.
	uv run etl-mappings --registry $(MAPPINGS_REGISTRY) check

demo:  ## Seed the demo databases (var/dev), load them with CRM, reconcile.
	uv run python scripts/demo_crm.py --env dev
	uv run etl --env dev run crm.full_load || true
	uv run etl --env dev runs
	uv run etl --env dev quarantine list
	uv run etl --env dev cutover report crm || true

PARTITIONS ?= 1,4
bench:  ## crm.full_load rows/s (ROWS=, PARTITIONS=, SOURCE= TARGET= URLs).
	uv run python benchmarks/bench_load.py --rows $(or $(ROWS),200000) \
		--partitions '$(PARTITIONS)' \
		$(if $(SOURCE),--source '$(SOURCE)') $(if $(TARGET),--target '$(TARGET)')

# GHSA-cg8m-r9f2-5wm2 is in Black's GitHub Action, which nothing here uses;
# Black comes with pyink, a formatter of the dev tools, pinned by pyink.
AUDIT_IGNORES := --ignore-vuln GHSA-cg8m-r9f2-5wm2
# The tools of the checks, at fixed versions: a new release of a scanner
# must not change what passes without a commit saying so.
PIP_AUDIT := pip-audit==2.10.1
BANDIT := bandit==1.9.4
CYCLONEDX := cyclonedx-bom==7.6.0

security:  ## Known vulnerabilities, insecure code, and the SBOM (var/).
	mkdir -p var
	uv export --frozen --all-extras --all-groups --no-emit-project \
		--format requirements-txt -q -o var/requirements-all.txt
	uvx --from $(PIP_AUDIT) pip-audit -r var/requirements-all.txt \
		--require-hashes --disable-pip --progress-spinner off $(AUDIT_IGNORES)
	uvx --from $(BANDIT) bandit -r src scripts benchmarks -q
	# What ships: the extras, without the dev tools.
	uv export --frozen --all-extras --no-dev --no-emit-project \
		--format requirements-txt -q -o var/requirements.txt
	uvx --from $(CYCLONEDX) cyclonedx-py requirements var/requirements.txt \
		--of JSON -o var/sbom.cdx.json

docs:  ## Build the documentation site into docs/_build/html (warnings fail).
	uv run --group docs sphinx-build -W -q -b html docs docs/_build/html

check: lint typecheck test mappings  ## The quick gate; CI adds the databases, the coverage floor, security, docs, the image.

# Images of the tools, pinned by digest.
OTELCOL_IMAGE := docker.io/otel/opentelemetry-collector-contrib:0.164.0@sha256:340c9ca6a36617084c6661bdc4f0fe92b7cda5a805d61673ec8db375fddc522b
PROMETHEUS_IMAGE := docker.io/prom/prometheus:v3.15.1@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2
TRIVY_IMAGE := docker.io/aquasec/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c
SYFT_IMAGE := docker.io/anchore/syft:v1.56.0@sha256:697ae864af1299cfa43c54b161cea3dd65c6d54de82e3dba66ea3dccb3acc313

db-up:  ## Start every test database (MongoDB as a replica set), Seq and Kafka.
	$(CONTAINER) run -d --rm --name etl-test-postgres -p 55432:5432 \
		-e POSTGRES_USER=etl -e POSTGRES_PASSWORD=etl -e POSTGRES_DB=etl \
		docker.io/library/postgres:18 -c wal_level=logical
	$(CONTAINER) run -d --rm --name etl-test-mssql -p 51433:1433 \
		-e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=$(MSSQL_PASSWORD)' \
		mcr.microsoft.com/mssql/server:2022-latest
	$(CONTAINER) run -d --rm --name etl-test-oracle -p 51521:1521 \
		-e ORACLE_PASSWORD=etl -e APP_USER=etl -e APP_USER_PASSWORD=etl \
		docker.io/gvenzl/oracle-free:23-slim
	$(CONTAINER) run -d --rm --name etl-test-mysql -p 53306:3306 \
		-e MYSQL_ROOT_PASSWORD=etl -e MYSQL_DATABASE=etl \
		-e MYSQL_USER=etl -e MYSQL_PASSWORD=etl \
		docker.io/library/mysql:8.4
	$(CONTAINER) run -d --rm --name etl-test-mongodb -p 57017:27017 \
		docker.io/library/mongo:8.2 --replSet rs0 --bind_ip_all
	$(CONTAINER) run -d --rm --name etl-test-seq -p 55341:80 \
		-e ACCEPT_EULA=Y -e SEQ_FIRSTRUN_NOAUTHENTICATION=true \
		docker.io/datalust/seq:latest
	# One KRaft node; it reaches itself on the port it advertises.
	$(CONTAINER) run -d --rm --name etl-test-kafka -p 59092:59092 \
		-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:59092,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:59092 \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
		docker.io/apache/kafka:3.9.1
	$(CONTAINER) run -d --rm --name etl-test-vault -p 58200:8200 \
		-e VAULT_DEV_ROOT_TOKEN_ID=root -e VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200 \
		docker.io/hashicorp/vault:1.20
	# A collector writing every signal it receives to var/otel-test.
	mkdir -p var/otel-test && chmod 777 var/otel-test
	$(CONTAINER) run -d --rm --name etl-test-otel -p 54318:4318 \
		-v $(CURDIR)/deploy/otel-collector/file.yaml:/etc/otelcol-contrib/config.yaml:Z \
		-v $(CURDIR)/var/otel-test:/out:Z \
		$(OTELCOL_IMAGE)
	until $(CONTAINER) exec etl-test-mongodb mongosh --quiet --eval \
		"try { rs.status().ok } catch (e) { rs.initiate().ok }" \
		>/dev/null 2>&1; do sleep 2; done
	$(MAKE) db-wait

db-wait:  ## Wait until the test databases accept connections.
	$(DB_URLS) uv run python scripts/wait_for_databases.py

db-down:  ## Stop and remove the test databases.
	$(CONTAINER) rm -f etl-test-postgres etl-test-mssql etl-test-oracle \
		etl-test-mysql etl-test-mongodb etl-test-seq etl-test-kafka \
		etl-test-otel etl-test-vault

otel-validate:  ## Check the collector configurations and the alerting rules.
	for config in deploy/otel-collector/*.yaml; do \
		$(CONTAINER) run --rm -e MY_POD_IP=10.0.0.1 \
			-e ELASTICSEARCH_URL=http://es:9200 \
			-e ELASTICSEARCH_API_KEY=key -e LOKI_URL=http://loki:3100 \
			-e SEQ_URL=http://seq:5341 -e SEQ_API_KEY=key \
			-e TEMPO_URL=http://tempo:4318 -e PROMETHEUS_URL=http://prometheus:9090 \
			-v $$PWD/$$config:/config.yaml:Z \
			$(OTELCOL_IMAGE) validate --config=/config.yaml || exit 1; \
	done
	$(CONTAINER) run --rm -v $$PWD/deploy/grafana/alerts.yaml:/alerts.yaml:Z \
		--entrypoint promtool $(PROMETHEUS_IMAGE) check rules /alerts.yaml

# The image, and the demo on a local Kubernetes (kind), as a CronJob's Job and
# as an Airflow DAG whose tasks are pods.
IMAGE ?= localhost/etl-pipeline:dev
IMAGE_ARGS ?=
KIND_CLUSTER ?= etl
AIRFLOW_IMAGE ?= docker.io/apache/airflow:3.3.2
KUBECTL := kubectl --context kind-$(KIND_CLUSTER) -n etl-demo
ifeq ($(notdir $(CONTAINER)),podman)
# Rootless podman: kind needs the cgroup controllers delegated to its scope.
KIND_ENV := KIND_EXPERIMENTAL_PROVIDER=podman
KIND_SCOPE := systemd-run --scope --user -p Delegate=yes
endif

image:  ## Build the image (IMAGE=, IMAGE_ARGS= more build arguments).
	$(CONTAINER) build $(IMAGE_ARGS) -t $(IMAGE) .

image-tar: image
	mkdir -p var && rm -f var/image.tar
	$(CONTAINER) save $(IMAGE) -o var/image.tar

# The vulnerabilities of the image, OS packages and Python libraries (even
# vendored ones) included, that have a fix: high or critical fail. Ignores,
# each with its reason: .trivyignore.
image-scan: image-tar  ## Scan the image for fixable vulnerabilities (trivy).
	$(CONTAINER) run --rm -v $(CURDIR):/work:Z -w /work $(TRIVY_IMAGE) \
		image --input var/image.tar --cache-dir var/trivy-cache \
		--ignorefile .trivyignore --ignore-unfixed \
		--severity HIGH,CRITICAL --exit-code 1 --no-progress

image-sbom: image-tar  ## The SBOM of the image, OS packages included (var/).
	$(CONTAINER) run --rm -v $(CURDIR)/var:/work:Z $(SYFT_IMAGE) \
		docker-archive:/work/image.tar -o cyclonedx-json=/work/image.cdx.json

kind-up:  ## Start a local Kubernetes cluster (kind).
	$(KIND_ENV) $(KIND_SCOPE) kind create cluster --name $(KIND_CLUSTER) --wait 180s

kind-load: image-tar
	$(KIND_ENV) kind load image-archive var/image.tar --name $(KIND_CLUSTER)

k8s-demo: kind-load  ## Run the demo as a Job of the crm-full-load CronJob.
	kubectl --context kind-$(KIND_CLUSTER) apply -k deploy/k8s/overlays/demo
	$(KUBECTL) delete job crm-full-load-demo --ignore-not-found --wait
	$(KUBECTL) create job --from=cronjob/crm-full-load crm-full-load-demo
	$(KUBECTL) wait --for=condition=Complete job/crm-full-load-demo --timeout=300s
	$(KUBECTL) logs job/crm-full-load-demo -c etl | tail -3

airflow-demo: kind-load  ## Run the CRM DAG in Airflow, its tasks as pods.
	kubectl --context kind-$(KIND_CLUSTER) apply -k deploy/k8s/overlays/demo
	kubectl --context kind-$(KIND_CLUSTER) apply -f deploy/airflow/demo/pvc.yaml
	$(KIND_ENV) kind get kubeconfig --name $(KIND_CLUSTER) > var/kubeconfig
	$(CONTAINER) run --rm --network host \
		-v $(CURDIR)/deploy/airflow/dags:/opt/airflow/dags:ro,Z \
		-v $(CURDIR)/deploy/airflow/demo:/opt/airflow/demo:ro,Z \
		-v $(CURDIR)/var/kubeconfig:/opt/airflow/kubeconfig:ro,Z \
		-e KUBECONFIG=/opt/airflow/kubeconfig -e ETL_IMAGE=$(IMAGE) \
		-e ETL_NAMESPACE=etl-demo -e ETL_SEED_DEMO=1 \
		-e ETL_POD_TEMPLATE=/opt/airflow/demo/etl_pod.yaml $(AIRFLOW_IMAGE) \
		bash -c "airflow db migrate >/dev/null && airflow dags test etl_crm"

kind-down:  ## Delete the local Kubernetes cluster.
	$(KIND_ENV) kind delete cluster --name $(KIND_CLUSTER)

# `make -s print-OTELCOL_IMAGE`: a variable's value, for CI.
print-%:
	@echo '$($*)'

clean:
	rm -rf .mypy_cache .pytest_cache .coverage .coverage.* htmlcov build dist
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
