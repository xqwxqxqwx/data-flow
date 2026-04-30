import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main() -> None:
    raw_bucket = env("MINIO_RAW_BUCKET", "raw")
    input_prefix = env("BRONZE_INPUT_PREFIX", "bronze/orders")
    source_path = f"s3a://{raw_bucket}/{input_prefix}/"
    target_table = env("TARGET_ICEBERG_TABLE", "local.silver.orders_stream")

    spark = SparkSession.builder.appName("bronze_orders_to_iceberg_silver").getOrCreate()

    df = spark.read.json(source_path)
    clean_df = (
        df.select(
            col("event_id").cast("bigint").alias("event_id"),
            col("user_id").cast("bigint").alias("user_id"),
            to_timestamp("order_ts").alias("order_ts"),
            col("amount").cast("decimal(12,2)").alias("amount"),
            col("currency").cast("string").alias("currency"),
            to_timestamp("_ingest_ts").alias("ingest_ts"),
        )
        .dropna(subset=["event_id", "user_id", "order_ts", "amount", "currency"])
        .dropDuplicates(["event_id"])
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    exists = spark.catalog.tableExists(target_table)
    if exists:
        clean_df.writeTo(target_table).append()
    else:
        (
            clean_df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

    spark.stop()


if __name__ == "__main__":
    main()
