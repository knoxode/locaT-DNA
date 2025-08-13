FROM ubuntu:latest

# Set noninteractive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y python3 \
    python3-pip \
    python3-venv \
    wget \
    unzip \
    curl \
    samtools
    
RUN apt-get clean

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

# Install cron
RUN apt-get update && \
    apt-get install -y cron

# Copy your app code (already present)
WORKDIR /app
COPY src /app/src

# Copy sources.yaml to /var/lib/locaT-DNA before cache script runs
RUN mkdir -p /data/genome_cache
COPY src/app/genome_database/sources.yaml /data/genome_cache/sources.yaml
