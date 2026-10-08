#!/usr/bin/env bash
# run_all.sh — runs the full ETL pipeline, then tells you how to start the app.
# Usage: bash run_all.sh

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# Set JAVA_HOME to the local JDK bundled in the project
export JAVA_HOME="$PROJECT_DIR/.java_local/current"
export PATH="$JAVA_HOME/bin:$PATH"

# Keep kagglehub cache inside the project
export KAGGLEHUB_CACHE="$PROJECT_DIR/.cache/kagglehub"

# Activate the virtual environment
source "$PROJECT_DIR/.venv/bin/activate"

echo "============================================"
echo "Indian Railways Train Schedule Analytics"
echo "============================================"
echo ""
echo "Java:   $(java -version 2>&1 | head -1)"
echo "Python: $(python3 --version)"
echo ""

# Phase 0: Download data
echo ">>> Phase 0: Downloading data …"
python3 src/00_download.py
echo ""

# Phase 1: Inspect raw data
echo ">>> Phase 1: Inspecting raw data …"
python3 src/01_inspect.py
echo ""

# Phase 2: Clean and transform
echo ">>> Phase 2: Cleaning data …"
python3 src/02_clean.py
echo ""

# Phase 3: Analyse
echo ">>> Phase 3: Analysing data …"
python3 src/03_analyse.py
echo ""

echo "============================================"
echo "Pipeline complete!"
echo "============================================"
echo ""
echo "To start the website:"
echo "  source .venv/bin/activate"
echo "  streamlit run app.py"
echo ""
