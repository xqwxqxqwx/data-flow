import os

from pyspark.sql import SparkSession


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main() -> None:
    pg_host = env("SOURCE_PG_HOST", "postgres-source")
    pg_port = env("SOURCE_PG_PORT", "5432")
    pg_db = env("SOURCE_PG_DB", "source")
    pg_user = env("SOURCE_PG_USER", "source")
    pg_password = env("SOURCE_PG_PASSWORD", "source")
    incremental_from_ts = os.getenv("INCREMENTAL_FROM_TS")
    target_table = env("TARGET_ICEBERG_TABLE", "local.raw.orders")

    spark = (
        SparkSession.builder.appName("postgres_to_iceberg")
        # Iceberg catalog configured via spark-defaults.conf
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    dbtable = "raw.orders"
    if incremental_from_ts:
        # Expect ISO timestamp, e.g. 2026-01-01T00:00:00+00:00
        dbtable = (
            "(SELECT order_id, user_id, order_ts, amount, currency "
            f"FROM raw.orders WHERE order_ts > TIMESTAMP '{incremental_from_ts}') AS t"
        )

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.raw")

    exists = spark.catalog.tableExists(target_table)
    if incremental_from_ts and exists:
        df.writeTo(target_table).append()
    else:
        (
            df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

    spark.stop()


if __name__ == "__main__":
    main()
