# Databricks notebook source
# Notebook 01 — Prepare Bronze
#
# Odpowiedzialność:
#   - Wczytanie wybranego batcha JSON z Unity Catalog Volume
#   - Jawny schema (schema enforcement — nowe pola jak source_priority są ignorowane)
#   - Dodanie kolumn technicznych: ingested_at, source_file, run_id
#   - Idempotentny zapis do Delta przez MERGE po event_id
#
# Grain tabeli: 1 wiersz = 1 event (event_id)
# Tabela: workspace.de_course_module_03_project.bronze_order_events

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md ## 1. Parametry

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "workspace")
dbutils.widgets.text("schema_name",    "de_course_module_03_project")
dbutils.widgets.text("data_base_path", "/Volumes/workspace/de_course_module_03_project/raw_data/data")
dbutils.widgets.text("batch_id",       "batch_001")
dbutils.widgets.text("run_id",         "manual_run")

catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")
data_base_path = dbutils.widgets.get("data_base_path")
batch_id       = dbutils.widgets.get("batch_id")
run_id         = dbutils.widgets.get("run_id")

# Fallback — jeśli catalog_name pusty, pobierz aktualny
if not catalog_name:
    catalog_name = spark.sql("SELECT current_catalog()").first()[0]

database_name = f"{catalog_name}.{schema_name}"
bronze_table  = f"{database_name}.bronze_order_events"
source_path   = f"{data_base_path}/orders/{batch_id}_orders.json"

print(f"catalog   : {catalog_name}")
print(f"schema    : {schema_name}")
print(f"table     : {bronze_table}")
print(f"batch_id  : {batch_id}")
print(f"run_id    : {run_id}")
print(f"source    : {source_path}")

# COMMAND ----------

# MAGIC %md ## 2. Setup — schema i tabela Delta

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}")

# COMMAND ----------

# MAGIC %md ## 3. Jawny schema dla JSON

# COMMAND ----------

# Dlaczego jawny schema:
#   - Spark nie skanuje całego pliku żeby wykryć typy — szybciej i deterministycznie
#   - Pola spoza schematu (np. source_priority z batch_003) są ignorowane
#     — to świadoma decyzja: schema enforcement na warstwie Bronze
#   - event_ts wczytujemy jako String, konwertujemy do Timestamp przez to_timestamp()

# COMMAND ----------

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

# MAGIC %md ## 4. Wczytanie batcha i dodanie kolumn technicznych

# COMMAND ----------

raw_df = (
    spark.read
    .schema(order_schema)
    .json(source_path)
    .withColumn("event_ts",    F.to_timestamp("event_ts"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("run_id",      F.lit(run_id))
)

print(f"Rekordów w batchu: {raw_df.count()}")
display(raw_df)

# COMMAND ----------

# MAGIC %md ## 5. Idempotentny zapis do Bronze — MERGE po event_id

# COMMAND ----------

# Dlaczego MERGE, a nie INSERT lub overwrite:
#   - WHEN NOT MATCHED THEN INSERT — wstawia tylko nowe eventy
#   - Brak WHEN MATCHED UPDATE — Bronze jest append-only, nie nadpisujemy raw danych
#   - Uruchomienie tego samego batcha dwa razy nie duplikuje danych
#   - MERGE jest atomiczny — bezpieczny przy równoległych uruchomieniach

# COMMAND ----------

if not spark.catalog.tableExists(bronze_table):
    # Pierwsze uruchomienie — tabela nie istnieje, tworzymy ją przez zapis
    print(f"Tabela {bronze_table} nie istnieje — tworzę i ładuję dane.")
    (
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_table)
    )
else:
    # Kolejne uruchomienia — MERGE po event_id zapewnia idempotencję
    print(f"Tabela {bronze_table} istnieje — wykonuję MERGE po event_id.")
    raw_df.createOrReplaceTempView("bronze_incoming")

    spark.sql(f"""
        MERGE INTO {bronze_table} AS target
        USING bronze_incoming AS source
          ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md ## 6. Weryfikacja — stan tabeli po ingeście

# COMMAND ----------

result_df = spark.table(bronze_table)
print(f"Łączna liczba rekordów w Bronze: {result_df.count()}")
display(result_df.orderBy("event_ts"))

# COMMAND ----------

# MAGIC %md ## 7. DESCRIBE HISTORY — audyt operacji na tabeli Delta

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))

# COMMAND ----------

# MAGIC %md ## 8. Notatka: schema enforcement z batch_003

# COMMAND ----------

# batch_003 zawiera pole source_priority, którego nie ma w order_schema.
# Spark z jawnym schematem ignoruje nieznane pola — kolumna NIE pojawia się w tabeli.
# To jest pożądane zachowanie na warstwie Bronze (schema enforcement).
#
# Żeby świadomie dodać kolumnę (schema evolution), wykonaj poniższe i ponownie
# wczytaj batch_003 z rozszerzonym schematem order_schema:
#
#   spark.sql(f"ALTER TABLE {bronze_table} ADD COLUMN source_priority STRING")
#
# Decyzja w tym projekcie: source_priority zostaje poza Bronze.
# Uzasadnienie: pole nie jest wymagane przez żadną warstwę downstream.