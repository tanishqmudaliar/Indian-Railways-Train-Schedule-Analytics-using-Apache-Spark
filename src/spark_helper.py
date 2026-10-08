"""
spark_helper.py
---------------
Creates and returns a local-mode Spark session configured for this project.
Every script imports this so that Spark settings stay consistent.
"""

import os
from pyspark.sql import SparkSession


def get_spark(app_name="IndianRailwaysAnalytics"):
    """
    Build a SparkSession running in local mode with sensible defaults
    for a small-to-medium dataset on a Windows-mounted WSL drive.
    """

    # Resolve the project root (one level above src/)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Spark scratch directory — kept inside the project
    spark_tmp = os.path.join(project_root, ".spark_tmp")
    os.makedirs(spark_tmp, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Keep shuffle partitions small — the data fits on one machine
        # and the project folder is on a Windows drive (slower I/O).
        .config("spark.sql.shuffle.partitions", "8")
        # Scratch / temp directory inside the project (relative path, no spaces)
        .config("spark.local.dir", "./.spark_tmp")
        # Derby metastore inside the project (relative path, no spaces)
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=./metastore_db")
        # Warehouse directory inside the project (relative path, no spaces)
        .config("spark.sql.warehouse.dir", "./spark-warehouse")
        # Suppress verbose Spark logs
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Reduce console noise: show only warnings and errors
    spark.sparkContext.setLogLevel("WARN")

    return spark
