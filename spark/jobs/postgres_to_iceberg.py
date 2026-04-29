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

    spark = (
        SparkSession.builder.appName("postgres_to_iceberg")
        # Iceberg catalog configured via spark-defaults.conf
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw.orders")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.raw")
    (
        df.writeTo("local.raw.orders")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    spark.stop()


if __name__ == "__main__":
    main()
