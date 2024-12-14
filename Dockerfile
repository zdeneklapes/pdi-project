FROM ubuntu:24.10 AS app

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EDITOR=vim \
    PROJECT_DIR=/opt/app

#    PATH="/opt/app/.venv/bin:$PATH" \

# Update system packages and install dependencies
RUN <<EOF
    set -ex

    # Update and install essential tools
    apt-get update && apt-get install -y \
        procps \
        gettext \
        vim \
        fish \
        python3-dev \
        graphviz \
        libgraphviz-dev \
        bat \
        tree \
        cron \
        wget \
        curl \
        lsb-release \
        gcc \
        git \
        make \
        software-properties-common \
    --no-install-recommends

    # Install Node.js
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
    apt-get install -y nodejs

    # Clean up apt cache to reduce image size
    rm -rf /var/lib/apt/lists/*
EOF

# JAVA
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64/ \
    PATH=$JAVA_HOME/bin:$PATH
RUN <<EOF
    set -ex
    # Install OpenJDK 11
    apt-get update && apt-get install -y openjdk-21-jdk

    # Clean up
    rm -rf /var/lib/apt/lists/*
EOF

# Python
RUN <<EOF
    set -ex
#    add-apt-repository ppa:deadsnakes/ppa -y
    apt-get update
    apt-get install -y python3 python3-venv python3-pip
EOF

# Set working directory
WORKDIR $PROJECT_DIR

# Copy project dependency files
COPY pyproject.toml .python-version $PROJECT_DIR/

# Install Python dependencies
RUN <<EOF
    set -ex
    pip install --upgrade uv --break-system-packages
    uv sync
EOF
