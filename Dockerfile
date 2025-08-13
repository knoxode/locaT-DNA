FROM ubuntu:latest

# Set noninteractive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies via apt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-venv \
      wget \
      unzip \
      curl \
      samtools \
      build-essential \
      pkg-config \
      autoconf automake libtool \
      zlib1g-dev \
      libbz2-dev \
      liblzma-dev \
      libzstd-dev \
      libdeflate-dev \
      libcurl4-openssl-dev \
      libssl-dev \
      ca-certificates \
      cron && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Build and install HTSlib from source
ARG HTSLIB_VERSION=1.22.1
RUN wget -q https://github.com/samtools/htslib/releases/download/${HTSLIB_VERSION}/htslib-${HTSLIB_VERSION}.tar.bz2 && \
    tar -xjf htslib-${HTSLIB_VERSION}.tar.bz2 && \
    cd htslib-${HTSLIB_VERSION} && \
    ./configure --enable-libcurl --enable-plugins && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig && \
    cd .. && rm -rf htslib-${HTSLIB_VERSION} htslib-${HTSLIB_VERSION}.tar.bz2

# Set up Python virtual environment
WORKDIR /app
RUN python3 -m venv /app/venv

# Activate venv and install Python dependencies
COPY requirements.txt /app/
RUN /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r /app/requirements.txt

# Download JBrowse 2 web release and serve it
WORKDIR /opt
RUN wget https://github.com/GMOD/jbrowse-components/releases/download/v3.6.4/jbrowse-web-v3.6.4.zip && \
    unzip jbrowse-web-v3.6.4.zip -d jbrowse2 && \
    rm jbrowse-web-v3.6.4.zip

# Expose ports for Streamlit and JBrowse
EXPOSE 8501 3000

# Copy your app code
WORKDIR /app
COPY src /app/src

# Copy sources.yaml to /var/lib/locaT-DNA before cache script runs
RUN mkdir -p /data/genome_cache
COPY src/app/genome_database/sources.yaml /data/genome_cache/sources.yaml

ENV PATH="/app/venv/bin:${PATH}"

COPY entrypoint.sh /app/entrypoint.sh

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]