import os

from pyspark.sql import SparkSession
from src.application.use_cases import ETLUseCase
from src.infrastructure.adapters import PostgresJDBCWriter, S3JsonReader


def main():
    spark = SparkSession.builder.appName("FinancialLedgerReversals").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    db_url = os.getenv(
        "DB_URL",
        "jdbc:postgresql://superset-postgresql.data-platform.svc:5432/superset",
    )
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD", "admin123")
    source_path = os.getenv(
        "S3_SOURCE", "s3a://data-lake/eventos-financeiros/*/*/*/*.json"
    )
    target_table = os.getenv("TARGET_TABLE", "faturamento_contabil_diario")

    reader = S3JsonReader(spark)
    writer = PostgresJDBCWriter(db_url, db_user, db_password)

    use_case = ETLUseCase(reader, writer)
    use_case.execute(source_path, target_table)

    spark.stop()


if __name__ == "__main__":
    main()
