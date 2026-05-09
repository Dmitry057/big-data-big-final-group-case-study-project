#!/usr/bin/env python3

"""Measure Spark read time for one benchmark dataset stored in HDFS"""

import argparse
import json
import time

from pyspark.sql import SparkSession


TEAM = "team30"
WAREHOUSE = "project/hive/warehouse"
SPARK_SOURCE_FORMATS = {
    "avro": "com.databricks.spark.avro",
    "parquet": "parquet",
}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--format", required=True, choices=["avro", "parquet"])
    parser.add_argument("--path", required=True)
    parser.add_argument("--mode", required=True, choices=["full_scan", "analytic"])
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    """Create a Spark session using the course-provided pattern"""

    return (
        SparkSession.builder
        .appName(f"{TEAM} - spark benchmark")
        .master("yarn")
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.sql.avro.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )


def resolve_source_format(file_format: str) -> str:
    """Map the logical benchmark format to the Spark reader name."""

    return SPARK_SOURCE_FORMATS[file_format]


def main() -> None:
    """Run one timed Spark read benchmark"""

    args = parse_args()
    spark = build_spark_session()

    start = time.perf_counter()
    dataframe = spark.read.format(resolve_source_format(args.format)).load(args.path)

    if args.mode == "full_scan":
        result = dataframe.count()
    else:
        result = (
            dataframe
            .select("label", "proto", "orig_bytes", "resp_bytes")
            .where("label IS NOT NULL")
            .groupBy("label", "proto")
            .count()
            .count()
        )

    elapsed_seconds = time.perf_counter() - start
    print(
        "BENCHMARK_RESULT="
        + json.dumps(
            {
                "format": args.format,
                "path": args.path,
                "mode": args.mode,
                "result": result,
                "elapsed_seconds": elapsed_seconds,
            }
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
