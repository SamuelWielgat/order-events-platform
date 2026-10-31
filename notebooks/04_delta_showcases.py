# Databricks notebook source
# Notebook 04 — Delta Showcases (poza Jobem, uruchamiany ręcznie)
#
# Odpowiedzialność (Etap 5 planu):
#   - DESCRIBE HISTORY dla silver_orders_current (operacje MERGE)
#   - Time Travel: stan O-105 przed i po MERGE z batch_002 (PAID -> CANCELLED)
#   - Schema enforcement: zapis z dodatkową kolumną source_priority jest odrzucany
#   - Schema evolution: świadome dodanie kolumny przez mergeSchema
#
# Ten notebook NIE modyfikuje tabel pipeline'u. Enforcement i evolution są
# pokazane na tymczasowej tabeli, która jest usuwana na końcu.
# Uruchom po przetworzeniu batch_001 i batch_002 (min. dwie wersje silver).

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## 1. Parametry

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "workspace")
dbutils.widgets.text("schema_name",    "de_course_module_03_project")
dbutils.widgets.text("data_base_path", "/Volumes/workspace/de_course_module_03_project/raw_data/data")
dbutils.widgets.text("order_id",       "O-105")
dbutils.widgets.text("changed_status", "CANCELLED")

catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")
data_base_path = dbutils.widgets.get("data_base_path")
order_id       = dbutils.widgets.get("order_id")
changed_status = dbutils.widgets.get("changed_status")

if not catalog_name:
    catalog_name = spark.sql("SELECT current_catalog()").first()[0]

database_name       = f"{catalog_name}.{schema_name}"
bronze_table        = f"{database_name}.bronze_order_events"
silver_orders_table = f"{database_name}.silver_orders_current"
demo_table          = f"{database_name}.tmp_bronze_schema_demo"
batch_003_path      = f"{data_base_path}/orders/batch_003_orders.json"

print(f"silver_orders : {silver_orders_table}")
print(f"bronze        : {bronze_table}")
print(f"demo table    : {demo_table}")
print(f"order_id      : {order_id} (szukamy zmiany statusu na {changed_status})")

# COMMAND ----------
# MAGIC %md ## 2. DESCRIBE HISTORY — silver_orders_current
# MAGIC
# MAGIC Każda operacja na tabeli Delta (CREATE/WRITE, MERGE) to osobna wersja
# MAGIC z timestampem, użytkownikiem i metrykami (`operationMetrics`).

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {silver_orders_table}")
display(
    history_df
    .select("version", "timestamp", "userName", "operation", "operationParameters", "operationMetrics")
    .orderBy("version")
)

# COMMAND ----------
# MAGIC %md ## 3. Time Travel — przed i po MERGE
# MAGIC
# MAGIC Szukamy pierwszej wersji, w której zamówienie ma status docelowy
# MAGIC (domyślnie O-105: PAID -> CANCELLED w batch_002). Numer wersji wyznaczamy
# MAGIC z historii, a nie na sztywno — numeracja zależy od liczby wcześniejszych
# MAGIC uruchomień (np. powtórzony batch_002 też tworzy wersję MERGE).

# COMMAND ----------

versions = [r["version"] for r in history_df.select("version").orderBy("version").collect()]

after_version = None
for v in versions:
    row = spark.sql(
        f"SELECT status FROM {silver_orders_table} VERSION AS OF {v} WHERE order_id = '{order_id}'"
    ).first()
    if row is not None and row["status"] == changed_status:
        after_version = v
        break

if after_version is None or after_version == 0:
    print(
        f"Nie znaleziono zmiany statusu {order_id} -> {changed_status} w historii. "
        f"Uruchom najpierw batch_001 i batch_002."
    )
else:
    before_version = after_version - 1
    print(f"Wersja przed MERGE: {before_version}, wersja po MERGE: {after_version}")

    print(f"=== {order_id} — VERSION AS OF {before_version} (przed) ===")
    display(spark.sql(
        f"SELECT order_id, status, event_id, event_sequence, event_ts, batch_id "
        f"FROM {silver_orders_table} VERSION AS OF {before_version} WHERE order_id = '{order_id}'"
    ))

    print(f"=== {order_id} — VERSION AS OF {after_version} (po) ===")
    display(spark.sql(
        f"SELECT order_id, status, event_id, event_sequence, event_ts, batch_id "
        f"FROM {silver_orders_table} VERSION AS OF {after_version} WHERE order_id = '{order_id}'"
    ))

    print(f"=== Metryki operacji, która utworzyła wersję {after_version} ===")
    display(
        history_df
        .where(F.col("version") == after_version)
        .select("version", "operation", "operationMetrics")
    )

    # Wszystkie zamówienia zmienione lub dodane między wersjami (nie tylko wybrane)
    before_df = (
        spark.read.option("versionAsOf", before_version).table(silver_orders_table)
        .select("order_id", F.col("status").alias("status_before"),
                F.col("event_id").alias("event_id_before"))
    )
    after_df = (
        spark.read.option("versionAsOf", after_version).table(silver_orders_table)
        .select("order_id", F.col("status").alias("status_after"),
                F.col("event_id").alias("event_id_after"))
    )
    print(f"=== Zamówienia zmienione lub dodane między wersją {before_version} a {after_version} ===")
    display(
        after_df.join(before_df, on="order_id", how="left")
        .where(
            F.col("event_id_before").isNull()
            | (F.col("event_id_after") != F.col("event_id_before"))
        )
        .select("order_id", "status_before", "status_after", "event_id_before", "event_id_after")
        .orderBy("order_id")
    )

# Inne formy (przykłady składni):
#   SELECT * FROM <tabela> TIMESTAMP AS OF '2026-01-01T00:00:00'
#   RESTORE TABLE <tabela> TO VERSION AS OF <n>   -- cofnięcie tabeli (nie wykonujemy)

# COMMAND ----------
# MAGIC %md ## 4. Schema enforcement — Bronze nie ma `source_priority`
# MAGIC
# MAGIC Plik batch_003 zawiera pole `source_priority`, ale notebook 01 czyta JSON
# MAGIC z jawnym schematem, więc pole jest pomijane już przy odczycie.

# COMMAND ----------

if spark.catalog.tableExists(bronze_table):
    bronze_columns = spark.table(bronze_table).columns
    print(f"Kolumny Bronze: {bronze_columns}")
    print(f"source_priority w Bronze: {'source_priority' in bronze_columns}")
else:
    print(f"Tabela {bronze_table} nie istnieje — uruchom najpierw notebook 01.")

try:
    # Bez jawnego schematu Spark inferuje kolumny z pliku — widać pole spoza order_schema
    raw_batch_003_columns = spark.read.json(batch_003_path).columns
    print(f"Kolumny w pliku batch_003 (inferencja): {raw_batch_003_columns}")
    print(f"source_priority w pliku: {'source_priority' in raw_batch_003_columns}")
except Exception as e:
    print(f"Nie udało się odczytać {batch_003_path}: {type(e).__name__}")

# COMMAND ----------
# MAGIC %md ## 5. Enforcement na poziomie Delta + schema evolution
# MAGIC
# MAGIC Pokazujemy to na tymczasowej tabeli (struktura Bronze, 0 wierszy),
# MAGIC żeby nie zmieniać prawdziwego Bronze:
# MAGIC 1. zapis z nadmiarową kolumną bez `mergeSchema` -> Delta odrzuca zapis (enforcement),
# MAGIC 2. ten sam zapis z `mergeSchema=true` -> kolumna zostaje dodana (evolution).

# COMMAND ----------

try:
    spark.sql(f"CREATE OR REPLACE TABLE {demo_table} AS SELECT * FROM {bronze_table} LIMIT 0")
    print(f"Kolumny tabeli demo przed: {spark.table(demo_table).columns}")

    sample_df = (
        spark.table(bronze_table)
        .limit(1)
        .withColumn("source_priority", F.lit("HIGH"))
    )

    # 1) Enforcement — oczekujemy błędu niezgodności schematu
    try:
        sample_df.write.format("delta").mode("append").saveAsTable(demo_table)
        print("UWAGA: zapis się powiódł — enforcement nie zadziałał (sprawdź ustawienie autoMerge).")
    except Exception as e:
        print("ENFORCEMENT: zapis odrzucony przez Delta")
        print(f"  {type(e).__name__}: {str(e)[:400]}")

    print(f"Kolumny tabeli demo po odrzuconym zapisie: {spark.table(demo_table).columns}")

    # 2) Evolution — świadomie zezwalamy na zmianę schematu
    (
        sample_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(demo_table)
    )
    print("EVOLUTION: zapis z mergeSchema=true przeszedł")
    print(f"Kolumny tabeli demo po evolution: {spark.table(demo_table).columns}")
    display(spark.table(demo_table).select("event_id", "order_id", "source_priority"))

    display(
        spark.sql(f"DESCRIBE HISTORY {demo_table}")
        .select("version", "operation", "operationParameters")
        .orderBy("version")
    )
finally:
    spark.sql(f"DROP TABLE IF EXISTS {demo_table}")
    print(f"Usunięto tabelę demo {demo_table}")

# COMMAND ----------
# MAGIC %md ## 6. Podsumowanie decyzji
# MAGIC
# MAGIC - Bronze: schemat jawny, `source_priority` jest pomijane (enforcement) —
# MAGIC   żadna warstwa downstream tego pola nie potrzebuje.
# MAGIC - Gdyby pole było potrzebne: `ALTER TABLE <bronze> ADD COLUMN source_priority STRING`
# MAGIC   albo zapis z `mergeSchema=true`, oraz rozszerzenie `order_schema` w notebooku 01.
# MAGIC - Evolution jest zawsze świadomą decyzją, a nie efektem ubocznym
# MAGIC   (`spark.databricks.delta.schema.autoMerge.enabled` pozostaje wyłączone).
