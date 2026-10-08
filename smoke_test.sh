#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

export JAVA_HOME="$SCRIPT_DIR/.java_local/current"
export PATH="$JAVA_HOME/bin:$PATH"

source "$SCRIPT_DIR/.venv/bin/activate"

python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SmokeTest').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print(f'Spark version: {spark.version}')
data = [(1, 'Indian'), (2, 'Railways'), (3, 'Apache Spark')]
df = spark.createDataFrame(data, ['id', 'word'])
df.show()
print('Smoke test PASSED!')
spark.stop()
"
