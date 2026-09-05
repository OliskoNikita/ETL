from pyspark.sql import SparkSession
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.task")

parser = argparse.ArgumentParser()
parser.add_argument('--jdbc-url', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-password', required=True)
parser.add_argument('--table-name', required=True)
parser.add_argument('--s3-path', required=True)

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("JdbcToS3Regions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

try:
    logger.info(f"Читаем таблицу {args.table_name} из PostgreSQL...")

    # Чтение из PostgreSQL
    jdbc_df = spark.read \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("dbtable", args.table_name) \
        .option("fetchsize", 1000) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    record_count = jdbc_df.count()
    logger.info(f"Прочитано записей: {record_count}")

    if record_count == 0:
        logger.warning("Таблица пустая. Записываем пустой датасет в S3.")

    logger.info(f"Записываем данные в {args.s3_path} (mode=overwrite)")

    jdbc_df.write \
        .mode("overwrite") \
        .parquet(args.s3_path)

    logger.info(f"Успешно загружено {record_count} записей в S3.")

except Exception as e:
    logger.error(f"Ошибка при загрузке regions: {e}", exc_info=True)
    raise

finally:
    spark.stop()