# Databricks notebook source
# Notebook 02 — Build Silver and Gold
#
# Odpowiedzialność:
#   - Wybór najnowszego eventu per order_id (Window Function + deduplikacja)
#   - Idempotentna aktualizacja silver_orders_current przez MERGE z guard clause
#   - Rozbicie items przez explode do silver_order_items_current
#   - Broadcast join z products, flagi jakości danych
#   - Budowa gold_daily_sales i gold_category_sales
#   - explain() dla Window, joinu i agregacji
#
# Grain tabel:
#   silver_orders_current      — 1 wiersz = 1 order_id (aktualny stan)
#   silver_order_items_current — 1 wiersz = 1 pozycja aktualnego zamówienia
#   gold_daily_sales           — 1 wiersz = 1 dzień sprzedaży
#   gold_category_sales        — 1 wiersz = 1 dzień + 1 kategoria produktu

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## 1. Parametry

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "workspace")
dbutils.widgets.text("schema_name",    "de_course_module_03_project")
dbutils.widgets.text("data_base_path", "/Volumes/workspace/de_course_module_03_project/raw_data/data")
dbutils.widgets.text("batch_id",       "batch_001")

catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")
data_base_path = dbutils.widgets.get("data_base_path")
batch_id       = dbutils.widgets.get("batch_id")

if not catalog_name:
    catalog_name = spark.sql("SELECT current_catalog()").first()[0]

database_name  = f"{catalog_name}.{schema_name}"

bronze_table        = f"{database_name}.bronze_order_events"
silver_orders_table = f"{database_name}.silver_orders_current"
silver_items_table  = f"{database_name}.silver_order_items_current"
gold_daily_table    = f"{database_name}.gold_daily_sales"
gold_category_table = f"{database_name}.gold_category_sales"
products_path       = f"{data_base_path}/reference/products.csv"

print(f"batch_id : {batch_id}")
print(f"bronze   : {bronze_table}")
print(f"silver_orders : {silver_orders_table}")
print(f"silver_items  : {silver_items_table}")
print(f"gold_daily    : {gold_daily_table}")
print(f"gold_category : {gold_category_table}")
print(f"products : {products_path}")

# COMMAND ----------
# MAGIC %md ## 2. Wczytanie aktualnego batcha z Bronze

# COMMAND ----------

bronze_df = spark.table(bronze_table).where(F.col("batch_id") == batch_id)
print(f"Eventów w batchu {batch_id}: {bronze_df.count()}")
display(bronze_df)

# COMMAND ----------
# MAGIC %md ## 3. Window Function — wybór najnowszego eventu per order_id

# COMMAND ----------

# Deterministyczny tie-breaker trzech poziomach:
#   1. event_ts DESC      — późniejszy timestamp = nowszy event
#   2. event_sequence DESC — wyższy sequence = nowszy (gdy ts równe)
#   3. event_id DESC      — ostatni fallback (lexicographic)
#
# Shuffle: PARTITION BY order_id logicznie wymaga zebrania wszystkich eventów
# jednego order_id razem (na dużych danych: Exchange hashpartitioning(order_id)).
# W explain() poniżej, na tym małym zbiorze, Photon pokazuje PhotonSort + PhotonWindow
# bez węzła Exchange — szczegóły w engineering_notes.md (sekcja Spark Explain).

dedup_window = Window.partitionBy("order_id").orderBy(
    F.col("event_ts").desc(),
    F.col("event_sequence").desc(),
    F.col("event_id").desc(),
)

latest_events_df = (
    bronze_df
    .withColumn("rn", F.row_number().over(dedup_window))
    .where(F.col("rn") == 1)
    .drop("rn")
)

print("=== EXPLAIN: Window Function (PARTITION BY order_id — sort/shuffle po kluczu) ===")
latest_events_df.explain("formatted")

print(f"Unikalnych order_id w batchu: {latest_events_df.count()}")
display(latest_events_df)

# COMMAND ----------
# MAGIC %md ## 4. MERGE do silver_orders_current
#
# Guard clause: aktualizuj tylko gdy nowy event jest nowszy od tego w Silver.
# Chroni przed late-arriving events — np. E-3001 (seq=0) nie nadpisze
# E-2001 (seq=2), który jest już w Silver.
#
# Porównanie to jedna krotka (struct) w tej samej kolejności co tie-breaker Window:
#   (event_ts, event_sequence, event_id)  — porównanie leksykograficzne.
# Dzięki temu MERGE i Window zawsze zgadzają się, który event jest nowszy.
# Ten sam event wczytany ponownie jest równy (nie większy), więc nie aktualizuje wiersza
# — idempotencja.
#
# Dlaczego MERGE, a nie overwrite:
#   - Zachowuje wiersze bez nowych eventów w batchgu (np. O-103 nie jest
#     w batch_002, ale musi pozostać w Silver)
#   - Atomiczny na poziomie wiersza

# COMMAND ----------

if not spark.catalog.tableExists(silver_orders_table):
    print(f"Tabela {silver_orders_table} nie istnieje — tworzę.")
    (
        latest_events_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_orders_table)
    )
else:
    print(f"Tabela {silver_orders_table} istnieje — wykonuję MERGE z guard clause.")
    latest_events_df.createOrReplaceTempView("silver_incoming")

    spark.sql(f"""
        MERGE INTO {silver_orders_table} AS target
        USING silver_incoming AS source
          ON target.order_id = source.order_id
        WHEN MATCHED AND struct(source.event_ts, source.event_sequence, source.event_id)
                       > struct(target.event_ts, target.event_sequence, target.event_id)
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

print(f"Rekordów w silver_orders_current: {spark.table(silver_orders_table).count()}")
display(spark.table(silver_orders_table).orderBy("order_id"))

# COMMAND ----------
# MAGIC %md ## 5. Wczytanie products — broadcast join

# COMMAND ----------

# products.csv ma ~5 wierszy — zawsze kwalifikuje się do broadcast join.
# Spark automatycznie wybierze BroadcastHashJoin (duża strona nie jest shufflowana).
# Wymuszamy broadcast explicite przez F.broadcast() — widoczne w explain() poniżej.

products_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(products_path)
)

print(f"Produktów w katalogu: {products_df.count()}")
display(products_df)

# COMMAND ----------
# MAGIC %md ## 6. Explode items + join products + flagi jakości

# COMMAND ----------

# Wczytujemy aktualny pełny Silver Orders (nie tylko batch) —
# silver_order_items_current musi zawierać pozycje WSZYSTKICH aktualnych zamowień.
silver_orders_full_df = spark.table(silver_orders_table)

items_exploded_df = (
    silver_orders_full_df
    # explode — każda pozycja z tablicy items staje się osobnym wierszem
    .withColumn("item",        F.explode("items"))
    .select(
        "order_id",
        "customer_id",
        "event_id",
        "event_ts",
        "event_sequence",
        "status",
        "currency",
        "ingested_at",
        "batch_id",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
    )
    # line_revenue — przychód z pozycji
    .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
)

# LEFT JOIN z products — broadcast po stronie products (mała tabela)
# Duża strona (pozycje zamówień) nie jest shufflowana. Widoczne w explain() poniżej.
silver_items_df = (
    items_exploded_df
    .join(F.broadcast(products_df), on="product_id", how="left")
    # product_missing: True gdy produkt nie istnieje w katalogu (np. P-404)
    .withColumn("product_missing", F.col("product_name").isNull())
    # is_valid_item: False gdy quantity <= 0, unit_price < 0 lub brakujący produkt
    # Rekordy invalid nie są usuwane — są oznaczane flagą i mierzone w quality check
    .withColumn(
        "is_valid_item",
        (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (~F.col("product_missing"))
    )
)

print("=== EXPLAIN: Broadcast join z products (BroadcastHashJoin) ===")
silver_items_df.explain("formatted")

print(f"Pozycji w silver_order_items_current: {silver_items_df.count()}")
display(silver_items_df.orderBy("order_id", "product_id"))

# COMMAND ----------
# MAGIC %md ## 7. Zapis silver_order_items_current — overwrite
#
# Dlaczego overwrite (nie MERGE):
#   - Po explode naturalnym kluczem byłoby (order_id, product_id), ale
#     ten sam produkt może wystąpić dwa razy w jednym zamówieniu
#   - Tabela zawsze odzwierciedla aktualny stan silver_orders_current
#   - Overwrite jest atomiczny w Delta — brak okna niesprawności

# COMMAND ----------

(
    silver_items_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_items_table)
)

print(f"Zapisano {spark.table(silver_items_table).count()} rekordów do {silver_items_table}")

# COMMAND ----------
# MAGIC %md ## 8. Notatka: cache na Serverless
#
# cache() / PERSIST TABLE nie jest wspierany na Databricks Serverless compute.
# Na klasycznym klastrze można byłoby użyć:
#   silver_items_cached_df = spark.table(silver_items_table).cache()
# żeby uniknąć podwójnego odczytu przy budowie dwóch tabel Gold.
#
# Na Serverless: obie tabele Gold czytają z zapisanej tabeli Delta —
# Delta jest zoptymalizowana pod odczyt i nie wymaga cache.

# COMMAND ----------
# MAGIC %md ## 9. Gold — gold_daily_sales
#
# Grain: 1 wiersz = 1 dzień sprzedaży
# Filtr: tylko zamówienia PAID + is_valid_item = True
# Źródło: silver_order_items_current (nie bezpośrednio Bronze)

# COMMAND ----------

daily_sales_df = (
    spark.table(silver_items_table)
    .where((F.col("status") == "PAID") & (F.col("is_valid_item") == True))
    .withColumn("order_date", F.to_date("event_ts"))
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("total_items"),
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
    )
    .orderBy("order_date")
)

print("=== EXPLAIN: Agregacja Gold daily (partial + final agregacja; Exchange zależy od rozmiaru danych) ===")
daily_sales_df.explain("formatted")

(
    daily_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_daily_table)
)

print(f"Zapisano {spark.table(gold_daily_table).count()} rekordów do {gold_daily_table}")
display(spark.table(gold_daily_table))

# COMMAND ----------
# MAGIC %md ## 10. Gold — gold_category_sales
#
# Grain: 1 wiersz = 1 dzień + 1 kategoria produktu
# Filtr: tylko zamówienia PAID + is_valid_item = True
# Brakujące produkty (product_missing=True) mają category=NULL
# — są zastępowane etykietą 'Unknown' zamiast wykluczania

# COMMAND ----------

category_sales_df = (
    spark.table(silver_items_table)
    .where((F.col("status") == "PAID") & (F.col("is_valid_item") == True))
    .withColumn("order_date", F.to_date("event_ts"))
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown")))
    .groupBy("order_date", "category")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("item_count"),
        F.round(F.sum("line_revenue"), 2).alias("category_revenue"),
    )
    .orderBy("order_date", "category")
)

(
    category_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_category_table)
)

print(f"Zapisano {spark.table(gold_category_table).count()} rekordów do {gold_category_table}")
display(spark.table(gold_category_table))

# COMMAND ----------
# MAGIC %md ## 11. DESCRIBE HISTORY — silver_orders_current
#
# Pokazuje historię operacji MERGE — każde uruchomienie batcha
# pozostawia ślad w historii Delta z timestampem i parametrami.
# Podstawa dla Time Travel w ETAP 5.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_orders_table}"))

