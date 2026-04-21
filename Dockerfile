FROM python:3.12-bookworm

ARG JULIA_VERSION=1.12.3
ARG JULIA_ARCH=x86_64
ARG JULIA_S3_ARCH=x64

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV JULIA_DEPOT_PATH=/opt/powermodelconverter/.julia_depot
ENV JULIA_BIN=/opt/julia/bin/julia
ENV PATH=/opt/julia/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        libgomp1 \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://julialang-s3.julialang.org/bin/linux/${JULIA_S3_ARCH}/${JULIA_VERSION%.*}/julia-${JULIA_VERSION}-linux-${JULIA_ARCH}.tar.gz" \
    | tar -xz -C /opt \
    && mv /opt/julia-${JULIA_VERSION} /opt/julia

WORKDIR /opt/powermodelconverter

COPY pyproject.toml README.md /opt/powermodelconverter/
COPY src /opt/powermodelconverter/src
COPY scripts /opt/powermodelconverter/scripts
COPY tests /opt/powermodelconverter/tests
COPY docs /opt/powermodelconverter/docs
COPY input /opt/powermodelconverter/input
COPY LICENSE CITATION.cff /opt/powermodelconverter/

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -e ".[dev]"

RUN bash scripts/bootstrap_julia_env.sh

COPY docker/entrypoint.sh /usr/local/bin/powermodelconverter-entrypoint
RUN chmod +x /usr/local/bin/powermodelconverter-entrypoint

WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/powermodelconverter-entrypoint", "pmc"]
CMD ["--help"]
