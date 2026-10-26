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
| `notebooks/03_quality_check.py` | ✅ Kompletny | ✅ batch_003 → FAILED: `Quality gate failed: 1 invalid quantity records found (max_invalid_items=0)` |
| `dlt/dlt_design_example.py` | ✅ Szkic gotowy (Auto Loader, apply_changes, expectations, Gold MV) | — nie deployowany ani uruchamiany (zgodnie z wymaganiami kursu) |
| `databricks.yml` | ✅ Kompletny (3 taski, parametry Joba, `base_parameters`) | ✅ `bundle validate`, `deploy`, `run` — failed i successful run potwierdzone przez użytkownika |

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

### Batch_003 — uruchomiony
- Notebooki: notebook 03 → FAILED z `Quality gate failed: 1 invalid quantity records found (max_invalid_items=0)` ✅
- Job (`databricks bundle run`): failed run (`max_invalid_items=0`) i successful run (`max_invalid_items=1`) ✅

batch_003 zawiera:
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

Zrobione: notebooki 01–03, `databricks.yml` (Etap 4), szkic DLT (Etap 6.1).

Zostało:
- Przykład Jobs API (Etap 6.2) — do opisania w `engineering_notes.md`
- Etap 5: DESCRIBE HISTORY, Time Travel, schema enforcement/evolution (notebook 02 ma już DESCRIBE HISTORY dla `silver_orders_current`; brakuje Time Travel i opisu schema evolution)
- Uzupełnij `engineering_notes.md` (Etap 7)
- Uzupełnij `self_review.md` (Etap 7)
- Opcjonalnie: quarantine table, `gold_data_quality_summary`

Uwaga: `silver_order_items_current` zawiera teraz rekord O-111 (qty=-1) z batch_003,
więc notebook 03 kończy się FAILED, dopóki `max_invalid_items=0`.

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
