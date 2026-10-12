# ==============================================================================
# Dockerfile: Indian Railways Train Schedule Analytics
# Multi-stage-ready image providing Python 3.12, OpenJDK 17, and Streamlit
# ==============================================================================

FROM python:3.12-slim-bookworm

# Metadata
LABEL maintainer="Tanishq Mudaliar"
LABEL description="Indian Railways Train Schedule Analytics using Apache Spark and Streamlit"

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# Install OpenJDK 17 runtime (required by Apache Spark) and curl (for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for optimal Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application assets, source code, tests, and pre-computed datasets
COPY . .

# Expose Streamlit default service port
EXPOSE 8501

# Container healthcheck querying Streamlit's internal health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Default runtime command: Launch Streamlit web dashboard
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
