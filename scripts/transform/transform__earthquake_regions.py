from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, split, trim, lower, md5, to_date, coalesce, lit, get, to_timestamp
)

import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument('--jdbc-url', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-password', required=True)
parser.add_argument('--table-name', required=True)
parser.add_argument('--s3-path-regions', required=True)
parser.add_argument('--s3-path-earthquake', required=True)

logger = logging.getLogger("airflow.task")
args = parser.parse_args()

# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquakeRegions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

try:
    # ------------------------------------------------------------------
    # 1. Получаем max(load_date) из ClickHouse
    # ------------------------------------------------------------------
    ch_max_df = spark.read \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("dbtable", f"(SELECT MAX(load_date) as max_date FROM {args.table_name})") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .load()

    max_date_val = ch_max_df.collect()[0]["max_date"]

    if max_date_val is None:
        max_date = "1970-01-01"
        logger.info("Таблица пустая или max(load_date) = null. Загружаем все данные.")
    else:
        max_date = max_date_val.strftime("%Y-%m-%d")
        logger.info(f"Последняя загруженная дата: {max_date}")

    # ------------------------------------------------------------------
    # 2. Читаем сырые данные из S3
    # ------------------------------------------------------------------
    raw_df = spark.read.json(args.s3_path_earthquake)

    if raw_df.rdd.isEmpty():
        logger.info("Нет новых файлов в S3. Завершаем работу.")
        spark.stop()
        exit(0)

    # ------------------------------------------------------------------
    # 3. Flatten + создание всех нужных колонок
    # ------------------------------------------------------------------
    flattened = raw_df.selectExpr("explode(features) as feature") \
        .select(
            col("feature.id").alias("id"),
            from_unixtime(col("feature.properties.time") / 1000).alias("ts"),
            to_date(from_unixtime(col("feature.properties.time") / 1000)).alias("load_date"),
            trim(get(split(col("feature.properties.place"), ","), 0)).alias("place"),
            trim(get(split(col("feature.properties.place"), ","), 1)).alias("initial_region"),
            col("feature.properties.mag").alias("magnitude"),
            col("feature.properties.felt").alias("felt"),
            col("feature.properties.tsunami").alias("tsunami"),
            col("feature.properties.url").alias("url"),
            get(col("feature.geometry.coordinates"), 0).alias("longitude"),
            get(col("feature.geometry.coordinates"), 1).alias("latitude"),
            get(col("feature.geometry.coordinates"), 2).alias("depth"),
            md5(lower(trim(col("feature.properties.place")))).alias("place_hash")
        )

    # Фильтрация
    if max_date != "1970-01-01":
        flattened = flattened.filter(col("load_date") > lit(max_date))
        logger.info(f"Фильтруем данные после {max_date}")

    # ------------------------------------------------------------------
    # 4. Читаем справочник регионов
    # ------------------------------------------------------------------
    regions = spark.read.parquet(args.s3_path_regions)

    # ------------------------------------------------------------------
    # 5. Обогащение (left join)
    # ------------------------------------------------------------------
    enriched = flattened.alias("f") \
        .join(regions.alias("r"), on="place_hash", how="left") \
        .select(
            col("f.id"),
            col("f.ts"),
            col("f.place"),
            coalesce(col("r.region"), col("f.initial_region")).alias("region"),
            col("f.magnitude"),
            col("f.felt"),
            col("f.tsunami"),
            col("f.url"),
            col("f.longitude"),
            col("f.latitude"),
            col("f.depth"),
            col("f.load_date")
        )

    record_count = enriched.count()
    logger.info(f"Подготовлено к загрузке записей: {record_count}")

    if record_count == 0:
        logger.info("Нет новых записей для загрузки.")
        spark.stop()
        exit(0)

    enriched = enriched.select(
        col("id").cast("string").alias("id"),
        to_timestamp(col("ts")).alias("ts"),
        col("place").cast("string").alias("place"),
        col("region").cast("string").alias("region"),
        col("magnitude").cast("float").alias("magnitude"),
        col("felt").cast("int").alias("felt"),
        coalesce(col("tsunami").cast("int"), lit(0)).alias("tsunami"),
        col("url").cast("string").alias("url"),
        col("longitude").cast("double").alias("longitude"),
        col("latitude").cast("double").alias("latitude"),
        col("depth").cast("double").alias("depth"),
        col("load_date").cast("date").alias("load_date"),
    )

    # ------------------------------------------------------------------
    # 6. Запись в ClickHouse
    # ------------------------------------------------------------------
    enriched.write \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("dbtable", args.table_name) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    logger.info(f"Успешно загружено {record_count} записей в ClickHouse.")

except Exception as e:
    logger.error(f"Ошибка при выполнении трансформации: {e}", exc_info=True)
    raise

finally:
    spark.stop()