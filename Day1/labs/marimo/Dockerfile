FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /usr/local/bin/uv

# UV_PROJECT is what makes a bare `uv run ...` work from /work. Compose mounts
# ./work over /work, so there is no pyproject.toml on the default search path and
# uv would otherwise build an empty ephemeral env - the labs then fail with
# ModuleNotFoundError on the first import.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PROJECT=/opt/app \
    UV_LINK_MODE=copy \
    JAVA_HOME=/usr/lib/jvm/default-java

# The uv project lives at /opt/app, NOT /work: compose bind-mounts ./work over
# /work, which would shadow pyproject.toml + uv.lock and leave `uv run` with no
# project to resolve marimo from.
WORKDIR /opt/app
COPY pyproject.toml /opt/app/pyproject.toml
RUN uv sync --no-dev

# Pre-warm the Ivy cache with the S3A jars lab 2.2 requests via
# spark.jars.packages. Without this, every learner downloads ~300 MB from
# repo1.maven.org mid-lab - and a whole room doing it at once on shared wifi is
# far worse than the ~50 s a single good link takes. Baking it moves that cost to
# lab 0 setup, the evening before. Coordinates MUST stay identical to the
# notebook's, or Ivy misses the cache and downloads again.
# The cache dir is Ivy-version-specific (Spark 4 ships Ivy 2.5.3, which uses
# /root/.ivy2.5.2, not /root/.ivy2) - hence the glob in the guard below.
RUN /opt/venv/bin/python -c "\
from pyspark.sql import SparkSession; \
s = SparkSession.builder.appName('warm-ivy-cache')\
.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52')\
.getOrCreate(); \
s.stop()" \
 && ls -d /root/.ivy*/jars

EXPOSE 8888
