# Databricks notebook source
# DLT Design Example — opis alternatywy w Delta Live Tables
#
# UWAGA: to szkic projektowy. NIE jest deployowany i NIE był uruchamiany —
# punkt wyjścia do opisu w engineering_notes.md. Aktualny pipeline to
# notebooki 01–03 uruchamiane przez Job (databricks.yml).
#
# Mapowanie na nasz pipeline:
#   notebook 01 (Bronze, MERGE po event_id)  -> bronze_order_events_dlt (Auto Loader)
#   notebook 02 (Window + MERGE z guard)      -> apply_changes
#   notebook 02 (explode + join + flagi)      -> silver_order_items_dlt
#   notebook 02 (Gold overwrite)              -> gold_daily_sales_dlt (materialized view)
#   notebook 03 (quality gate, raise)         -> expectations (@dlt.expect*)
#
# Grain tabel jak w wersji notebookowej:
#   bronze_order_events_dlt   — 1 wiersz = 1 event
#   silver_orders_current_dlt — 1 wiersz = 1 order_id (aktualny stan)
#   silver_order_items_dlt    — 1 wiersz = 1 pozycja aktualnego zamówienia
#   gold_daily_sales_dlt      — 1 wiersz = 1 dzień sprzedaży

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# Parametr pipeline'u (Pipeline settings -> Configuration), nie w kodzie:
#   data_base_path = /Volumes/workspace/de_course_module_03_project/raw_data/data
data_base_path = spark.conf.get("data_base_path")

# Ten sam jawny schema co w notebooku 01 (schema enforcement —
# nowe pola, np. source_priority z batch_003, są ignorowane).
item_schema = StructType([
    StructField("product_id", StringType(),  True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
])

order_schema = StructType([
    StructField("event_id",       StringType(),           False),
    StructField("batch_id",       StringType(),           False),
    StructField("source_system",  StringType(),           True),
    StructField("order_id",       StringType(),           True),
    StructField("customer_id",    StringType(),           True),
    StructField("event_ts",       StringType(),           True),
    StructField("event_sequence", IntegerType(),          True),
    StructField("status",         StringType(),           True),
    StructField("currency",       StringType(),           True),
    StructField("items",          ArrayType(item_schema), True),
])

# COMMAND ----------
# MAGIC %md ## Bronze — Auto Loader (streaming table)
# MAGIC
# MAGIC Auto Loader sam wykrywa nowe pliki w katalogu i przetwarza każdy dokładnie raz
# MAGIC (checkpoint) — zastępuje ręczny wybór pliku `{batch_id}_orders.json`
# MAGIC oraz idempotencję na poziomie plików.

# COMMAND ----------

@dlt.table(
    name="bronze_order_events_dlt",
    comment="Surowe eventy zamówień. Grain: 1 wiersz = 1 event.",
)
def bronze_order_events_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(order_schema)
        .load(f"{data_base_path}/orders/")
        .withColumn("event_ts",    F.to_timestamp("event_ts"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------
# MAGIC %md ## Silver orders — aktualny stan (apply_changes)
# MAGIC
# MAGIC `apply_changes` (CDC, SCD type 1) to deklaratywny odpowiednik
# MAGIC Window Function + MERGE z guard clause z notebooka 02:
# MAGIC - `keys` — klucz biznesowy (order_id),
# MAGIC - `sequence_by` — kolejność eventów; starszy event (np. E-3001, seq=0)
# MAGIC   nie nadpisze nowszego. Struct jest porównywany leksykograficznie, co odpowiada
# MAGIC   naszemu tie-breakerowi event_ts -> event_sequence -> event_id.
# MAGIC
# MAGIC Guard clause w notebooku 02 porównuje tę samą krotkę
# MAGIC (event_ts, event_sequence, event_id) — zgodność zachowania na danych
# MAGIC trzeba by jeszcze zweryfikować uruchamiając pipeline (szkic nie był uruchamiany).

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_orders_current_dlt",
    comment="Aktualny stan zamówienia. Grain: 1 wiersz = 1 order_id.",
)

dlt.apply_changes(
    target="silver_orders_current_dlt",
    source="bronze_order_events_dlt",
    keys=["order_id"],
    sequence_by=F.struct("event_ts", "event_sequence", "event_id"),
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md ## Silver items — materialized view z expectations
# MAGIC
# MAGIC Źródłem jest target `apply_changes` (ma update'y), więc czytamy batchowo
# MAGIC (`dlt.read`, nie `dlt.read_stream`). Tabela jest przeliczana z aktualnego stanu —
# MAGIC jak overwrite w notebooku 02.
# MAGIC
# MAGIC Expectations odpowiadają metrykom z notebooka 03:
# MAGIC - `valid_quantity`, `valid_price` -> `expect_or_fail`: zatrzymuje update pipeline'u
# MAGIC   (odpowiednik `raise Exception` przy max_invalid_items = 0),
# MAGIC - `product_exists` -> `expect`: tylko metryka w event logu, rekordy zostają
# MAGIC   (odpowiednik WARNING dla brakującego produktu, np. P-404).

# COMMAND ----------

@dlt.view(name="products_dlt")
def products_dlt():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{data_base_path}/reference/products.csv")
    )


@dlt.table(
    name="silver_order_items_dlt",
    comment="Pozycje aktualnych zamówień. Grain: 1 wiersz = 1 pozycja zamówienia.",
)
@dlt.expect_or_fail("valid_quantity", "quantity > 0")
@dlt.expect_or_fail("valid_price", "unit_price >= 0")
@dlt.expect("product_exists", "product_name IS NOT NULL")
def silver_order_items_dlt():
    return (
        dlt.read("silver_orders_current_dlt")
        .withColumn("item", F.explode("items"))
        .select(
            "order_id",
            "event_id",
            "event_ts",
            "status",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
        )
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        .join(F.broadcast(dlt.read("products_dlt")), on="product_id", how="left")
        .withColumn("product_missing", F.col("product_name").isNull())
    )

# COMMAND ----------
# MAGIC %md ## Gold — materialized view
# MAGIC
# MAGIC Deklaratywna agregacja z Silver (Gold nigdy bezpośrednio z Bronze).
# MAGIC Filtr jak w notebooku 02: tylko PAID i poprawne pozycje.

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales_dlt",
    comment="Sprzedaż dzienna (PAID). Grain: 1 wiersz = 1 dzień.",
)
def gold_daily_sales_dlt():
    return (
        dlt.read("silver_order_items_dlt")
        .where(
            (F.col("status") == "PAID")
            & (F.col("quantity") > 0)
            & (F.col("unit_price") >= 0)
            & (~F.col("product_missing"))
        )
        .withColumn("order_date", F.to_date("event_ts"))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("total_items"),
            F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        )
    )
