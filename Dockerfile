FROM python:3.11-slim-bullseye AS app

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EDITOR=vim \
    PATH="/app/.venv/bin:$PATH" \
    PROJECT_DIR=/opt/app

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
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/ \
    PATH=$JAVA_HOME/bin:$PATH
RUN <<EOF
    set -ex
    # Install OpenJDK 11
    apt-get update && apt-get install -y openjdk-17-jdk --no-install-recommends

    # Clean up
    rm -rf /var/lib/apt/lists/*
EOF


# Set working directory
WORKDIR $PROJECT_DIR

# Copy project dependency files
#COPY pyproject.toml requirements.txt .python-version ./
COPY pyproject.toml .python-version $PROJECT_DIR/

# Install Python dependencies
RUN <<EOF
    set -ex
    # Upgrade pip
    pip install --upgrade pip
    pip3 install uv

    uv sync
    # Install dependencies (uncomment if requirements.txt is available)
#     pip install -r requirements.txt
EOF

# Add any additional configurations or commands here if needed
