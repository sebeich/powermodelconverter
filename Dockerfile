FROM python:3.12-bookworm

ARG JULIA_VERSION=1.12.3
ARG JULIA_ARCH=x86_64
ARG JULIA_S3_ARCH=x64
ARG MATPOWER_VERSION=8.0

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV JULIA_DEPOT_PATH=/opt/powermodelconverter/.julia_depot
ENV JULIA_BIN=/opt/julia/bin/julia
ENV PATH=/opt/julia/bin:${PATH}
ENV MATPOWER_HOME=/opt/matpower

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        libgomp1 \
        octave \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://julialang-s3.julialang.org/bin/linux/${JULIA_S3_ARCH}/${JULIA_VERSION%.*}/julia-${JULIA_VERSION}-linux-${JULIA_ARCH}.tar.gz" \
    | tar -xz -C /opt \
    && mv /opt/julia-${JULIA_VERSION} /opt/julia

# MATPOWER on Octave: independent reference solver for MATPOWER import validation.
RUN curl -fsSL \
      "https://github.com/MATPOWER/matpower/archive/refs/tags/${MATPOWER_VERSION}.tar.gz" \
      -o /tmp/matpower.tar.gz \
    && mkdir -p /opt/matpower \
    && tar -xzf /tmp/matpower.tar.gz -C /opt/matpower --strip-components=1 \
    && rm /tmp/matpower.tar.gz

WORKDIR /opt/powermodelconverter

COPY pyproject.toml README.md /opt/powermodelconverter/
COPY src /opt/powermodelconverter/src
COPY scripts /opt/powermodelconverter/scripts
COPY tests /opt/powermodelconverter/tests
COPY docs /opt/powermodelconverter/docs
COPY LICENSE CITATION.cff /opt/powermodelconverter/

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -e ".[dev,ding0,reproduction]" \
    && python -m pip install --no-deps "ding0==0.2.1"

RUN bash scripts/bootstrap_julia_env.sh

COPY docker/entrypoint.sh /usr/local/bin/powermodelconverter-entrypoint
RUN chmod +x /usr/local/bin/powermodelconverter-entrypoint

WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/powermodelconverter-entrypoint", "pmc"]
CMD ["--help"]
