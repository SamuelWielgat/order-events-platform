# Databricks notebook source
# Notebook 03 — Quality Check
#
# Odpowiedzialność:
#   - Policzenie metryk jakości danych na silver_order_items_current
#   - Porównanie metryk z progami (widgety)
#   - Kontrolowane zatrzymanie Joba (raise Exception) przy przekroczeniu progu
#
# Metryki:
#   invalid_quantity_count — quantity <= 0          (np. O-111, E-3005, qty=-1)
#   invalid_price_count    — unit_price < 0
#   missing_product_count  — product_missing = True (np. O-109, P-404)
#
# Progi:
#   max_invalid_items     (default 0)  — dla invalid_quantity i invalid_price;
#                                        przekroczenie → FAIL
#   max_missing_products  (default 10) — przekroczenie → FAIL,
#                                        poniżej progu tylko WARNING
#                                        (P-404 to znany, oczekiwany przypadek)
#
# Quality gate jest hard stopem: w Jobie task ma max_retries = 0,
# bo ponowienie nie zmieni danych.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## 1. Parametry

# COMMAND ----------

dbutils.widgets.text("catalog_name",         "workspace")
dbutils.widgets.text("schema_name",          "de_course_module_03_project")
dbutils.widgets.text("batch_id",             "batch_001")
dbutils.widgets.text("max_invalid_items",    "0")
dbutils.widgets.text("max_missing_products", "10")

catalog_name         = dbutils.widgets.get("catalog_name")
schema_name          = dbutils.widgets.get("schema_name")
batch_id             = dbutils.widgets.get("batch_id")
max_invalid_items    = int(dbutils.widgets.get("max_invalid_items"))
max_missing_products = int(dbutils.widgets.get("max_missing_products"))

if not catalog_name:
    catalog_name = spark.sql("SELECT current_catalog()").first()[0]

database_name      = f"{catalog_name}.{schema_name}"
silver_items_table = f"{database_name}.silver_order_items_current"

print(f"batch_id             : {batch_id}")
print(f"silver_items         : {silver_items_table}")
print(f"max_invalid_items    : {max_invalid_items}")
print(f"max_missing_products : {max_missing_products}")

# COMMAND ----------
# MAGIC %md ## 2. Metryki jakości
# MAGIC
# MAGIC Źródło: `silver_order_items_current` — czytane bezpośrednio z Delta
# MAGIC (`cache()` nie jest wspierany na Serverless).
# MAGIC Wszystkie metryki liczone w jednym przebiegu agregacji globalnej.

# COMMAND ----------

silver_items_df = spark.table(silver_items_table)

metrics_row = (
    silver_items_df
    .agg(
        F.count("*").alias("total_items"),
        F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("invalid_quantity_count"),
        F.sum(F.when(F.col("unit_price") < 0, 1).otherwise(0)).alias("invalid_price_count"),
        F.sum(F.when(F.col("product_missing"), 1).otherwise(0)).alias("missing_product_count"),
    )
    .first()
)

# sum() zwraca NULL dla pustej tabeli — normalizujemy do 0
total_items            = int(metrics_row["total_items"] or 0)
invalid_quantity_count = int(metrics_row["invalid_quantity_count"] or 0)
invalid_price_count    = int(metrics_row["invalid_price_count"] or 0)
missing_product_count  = int(metrics_row["missing_product_count"] or 0)

# COMMAND ----------
# MAGIC %md ## 3. Quality summary

# COMMAND ----------

checks = [
    # (metric, value, threshold, threshold_widget)
    ("invalid_quantity_count", invalid_quantity_count, max_invalid_items,    "max_invalid_items"),
    ("invalid_price_count",    invalid_price_count,    max_invalid_items,    "max_invalid_items"),
    ("missing_product_count",  missing_product_count,  max_missing_products, "max_missing_products"),
]

summary_rows = []
for metric, value, threshold, widget in checks:
    if value > threshold:
        status = "FAIL"
    elif value > 0:
        status = "WARNING"
    else:
        status = "OK"
    summary_rows.append((metric, value, threshold, widget, status))

summary_df = spark.createDataFrame(
    summary_rows,
    "metric string, value long, threshold long, threshold_widget string, status string",
)

print(f"Quality summary — {silver_items_table} (batch_id={batch_id}, pozycji łącznie: {total_items})")
display(summary_df)

# Pozycje problematyczne — ułatwiają diagnozę w failed runie
problem_items_df = (
    silver_items_df
    .where(
        (F.col("quantity") <= 0)
        | (F.col("unit_price") < 0)
        | (F.col("product_missing"))
    )
    .select(
        "order_id", "product_id", "quantity", "unit_price",
        "product_missing", "is_valid_item", "status", "event_id", "batch_id",
    )
    .orderBy("order_id", "product_id")
)
display(problem_items_df)

# COMMAND ----------
# MAGIC %md ## 4. Quality gate
# MAGIC
# MAGIC Przekroczenie progu → `raise Exception` → task kończy się FAILED
# MAGIC i Job zatrzymuje się (kontrolowany failed run).
# MAGIC Brakujące produkty poniżej progu to tylko ostrzeżenie.

# COMMAND ----------

failures = []

if invalid_quantity_count > max_invalid_items:
    failures.append(
        f"{invalid_quantity_count} invalid quantity records found "
        f"(max_invalid_items={max_invalid_items})"
    )

if invalid_price_count > max_invalid_items:
    failures.append(
        f"{invalid_price_count} invalid price records found "
        f"(max_invalid_items={max_invalid_items})"
    )

if missing_product_count > max_missing_products:
    failures.append(
        f"{missing_product_count} missing product records found "
        f"(max_missing_products={max_missing_products})"
    )
elif missing_product_count > 0:
    print(
        f"WARNING: {missing_product_count} pozycji z brakującym produktem "
        f"(próg {max_missing_products}) — oczekiwany przypadek (np. P-404)."
    )

if failures:
    raise Exception("Quality gate failed: " + "; ".join(failures))

print("Quality gate PASSED")
