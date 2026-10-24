# Session Handoff — Order Events Platform

## Dla nowego agenta — przeczytaj to jako pierwsze

Projekt jest w trakcie implementacji. Poniżej pełny stan na moment przełączenia sesji.

---

## Stack i środowisko

- **IDE:** VS Code lokalnie, wtyczka agentowa (Continue → Cline)
- **Repo:** https://github.com/SamuelWielgat/order-events-platform (prywatne)
- **Databricks:** Free Edition, cloud.databricks.com, **Serverless compute + Photon engine**
- **Unity Catalog:** katalog `workspace`, schema `de_course_module_03_project`
- **Git Folders:** repo podłączone do Databricks — po `git push` robisz Pull w Databricks

---

## Ścieżki — krytyczne

| Co | Ścieżka |
|---|---|
| Volume (dane źródłowe) | `/Volumes/workspace/de_course_module_03_project/raw_data/data` |
| Orders batch | `{data_base_path}/orders/{batch_id}_orders.json` |
| Products | `{data_base_path}/reference/products.csv` |
| Bronze table | `workspace.de_course_module_03_project.bronze_order_events` |
| Silver orders | `workspace.de_course_module_03_project.silver_orders_current` |
| Silver items | `workspace.de_course_module_03_project.silver_order_items_current` |
| Gold daily | `workspace.de_course_module_03_project.gold_daily_sales` |
| Gold category | `workspace.de_course_module_03_project.gold_category_sales` |

---

## Stan notebooków

| Plik | Stan | Przetestowany |
|---|---|---|
| `notebooks/01_prepare_bronze.py` | ✅ Kompletny | ✅ batch_001, batch_002 × 2 (idempotencja) |
| `notebooks/02_build_silver_and_gold.py` | ✅ Kompletny | ✅ batch_001, batch_002 × 2 (idempotencja) |
| `notebooks/03_quality_check.py` | ✅ Zaimplementowany (do przetestowania w Databricks) | ❌ |
| `dlt/dlt_design_example.py` | ⏳ Szkielet — DO IMPLEMENTACJI | — |
| `databricks.yml` | ⏳ Szkielet — DO IMPLEMENTACJI | — |

---

## Co zostało przetestowane i działa

### Bronze (notebook 01)
- batch_001 → 4 rekordy ✅
- batch_002 → 8 rekordów ✅
- batch_002 ponownie → nadal 8 (idempotencja przez MERGE po `event_id`) ✅

### Silver + Gold (notebook 02)
- batch_001 → silver_orders: 4, silver_items: 5, gold_daily: 3, gold_category: 4 ✅
- batch_002 → silver_orders: 6, silver_items: 7, gold_daily: 3, gold_category: 3 ✅
  - O-101 zaktualizowany (seq=2 > seq=1) ✅
  - O-105 zmieniony PAID → CANCELLED ✅
  - O-108, O-109 dodane jako nowe ✅
  - O-103, O-106 nienaruszone (nie było ich w batch_002) ✅
- batch_002 ponownie → liczby bez zmian (idempotencja Silver) ✅

### Batch_003 — NIE uruchamiany jeszcze
Czeka na gotowy notebook 03 (quality_check). batch_003 zawiera:
- E-3001 dla O-101 (seq=0) — late-arriving event, guard clause powinien go odrzucić
- E-3002 dla O-108 → CANCELLED
- E-3003 + E-3004 dla O-110 — duplikat w batchu (dedup przed MERGE)
- `source_priority` — nowa kolumna (schema enforcement)
- E-3005 dla O-111 (qty=-1) — **celowo zły rekord → quality gate FAIL**

---

## Odkryte ograniczenia środowiska

| Ograniczenie | Szczegół | Rozwiązanie |
|---|---|---|
| `cache()` nie działa | Serverless nie wspiera PERSIST TABLE | Czytamy z Delta table bezpośrednio |
| `input_file_name()` deprecated w UC | Nie zwraca wartości przy Volumes | Używamy `F.col("_metadata.file_path")` |
| Explain bez `Exchange hashpartitioning` | Photon + mała ilość danych → `PhotonTopK` zamiast shuffle | Dokumentujemy w engineering notes |

---

## Co robić dalej — następny krok

**Zaimplementować `notebooks/03_quality_check.py`** zgodnie z `implementation_plan.md` Etap 3.

Logika:
1. Czytaj z `silver_order_items_current`
2. Policz: `invalid_quantity_count` (qty <= 0), `invalid_price_count` (unit_price < 0), `missing_product_count` (product_missing = True)
3. Widgety progów: `max_invalid_items` (default: 0), `max_missing_products` (default: 10)
4. `raise Exception` jeśli przekroczono próg → Job FAILS (kontrolowany)
5. Wyświetl quality summary

Po notebook 03:
- Uruchom batch_003 przez wszystkie trzy notebooki → zademonstruj failed run (qty=-1)
- Zaimplementuj `databricks.yml` (Etap 4)
- Uzupełnij `dlt/dlt_design_example.py` (Etap 6)
- Uzupełnij `engineering_notes.md` (Etap 7)
- Uzupełnij `self_review.md` (Etap 7)

---

## Jak pracujemy

1. Agent pisze/edytuje pliki lokalnie w VS Code
2. `git push` na GitHub
3. W Databricks: Pull w Git Folder → Run all w notebooku
4. Wyniki wracają do agenta przez chat

## Materiały kursu (tylko do wglądu, nie commitujemy)

Oryginalne wymagania i starter files:
```
~/Documents/kurs-data-engineering/projekt_3_local/materialy_kursu/module_03_project/
├── project_requirements.md
├── starter/project_tasks.md
└── starter/notebooks/   ← szkielety od prowadzącego (referencyjna)
```
