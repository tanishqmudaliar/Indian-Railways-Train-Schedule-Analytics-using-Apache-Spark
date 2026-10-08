#!/usr/bin/env bash
# setup_env.sh — creates the venv and installs dependencies
# Run from the project root directory

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

export JAVA_HOME="$PROJECT_DIR/.java_local/jdk-17.0.2"
export PATH="$JAVA_HOME/bin:$PATH"

echo "=== Java version ==="
java -version

echo ""
echo "=== Creating virtual environment ==="
python3 -m venv .venv
source .venv/bin/activate

echo "=== Installing packages ==="
pip install --cache-dir .cache/pip -q -r requirements.txt

echo ""
echo "=== Spark smoke test ==="
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SmokeTest').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print(f'Spark version: {spark.version}')
data = [(1, 'hello'), (2, 'spark')]
df = spark.createDataFrame(data, ['id', 'word'])
df.show()
print('Smoke test passed!')
spark.stop()
"

echo ""
echo "=== Setup complete ==="
