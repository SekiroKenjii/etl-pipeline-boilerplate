# The `etl` command, with every connector, as a non-root image.
#
#   docker build -t etl-pipeline:dev .
#   docker run --rm -e ETL_ENV=dev etl-pipeline:dev list
#
# Dependencies come from uv.lock, installed without the dev tools; the project
# is installed as a package (no source tree). Settings (config/) and mappings
# (mappings/) are baked in; deployments override them with mounted files and
# ETL__* variables, and provide secrets as files or from a secret manager.
#
# The base images are pinned by digest: a build takes the same bytes until
# the digest changes in a reviewed commit (Dependabot proposes it weekly).
# OS patches come the same way, not from `apt-get upgrade` here, so that a
# build stays reproducible: the official Python image is rebuilt on each
# update of its Debian base, and its new digest brings them. The image scan
# (`make image-scan`, in CI and before a release) fails on a vulnerability
# that has a fix: take the new digest; if no rebuilt base has the fix yet,
# `apt-get install --only-upgrade <package>` in the last stage is the
# stopgap, removed with the next digest.

FROM ghcr.io/astral-sh/uv:0.13.6@sha256:af22a52f5eafef3d78e551599d405c02df61919d18028f0cea8eda93bc9986d0 AS uv

FROM docker.io/library/python:3.14.10-slim-trixie@sha256:930e1abba7c4e8f96ed93e8012c35d174d7732752731bbd5e8e757d7ec6f3966 AS python

FROM python AS build
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/opt/etl
WORKDIR /src
# The extras to install: the SQL Server, Oracle and MySQL drivers by default.
ARG EXTRAS="--all-extras"
# Dependencies first: their layer survives changes of the code.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project ${EXTRAS}
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable ${EXTRAS}

FROM python
LABEL org.opencontainers.image.title="etl-pipeline" \
      org.opencontainers.image.description="ETL and data migration pipelines" \
      org.opencontainers.image.licenses="MIT"
# Running the ETL needs no pip (uv installed it): without pip and the
# libraries it vendors, there is less in the image to scan and patch.
RUN python -m pip uninstall --yes --root-user-action ignore pip \
    && rm -rf /usr/local/lib/python3.*/ensurepip/_bundled \
    && useradd --uid 10001 --user-group --no-create-home --home-dir /app etl
COPY --from=build /opt/etl /opt/etl
WORKDIR /app
COPY config ./config
COPY mappings ./mappings
ENV PATH=/opt/etl/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ETL_ENV=prod
# Settings and mappings readable by the user whatever their modes in the
# build context. File-based records and temporary files go to /app/var:
# mount a volume there when the root filesystem is read-only.
RUN chmod -R a+rX /app/config /app/mappings \
    && mkdir -p /app/var && chown etl:etl /app/var
USER 10001:10001
ENTRYPOINT ["etl"]
CMD ["--help"]
