# 🏗️ Plan Implementacji Pipeline'u: Databricks Order Events Platform

## SETUP — Środowisko Deweloperskie

Ten krok dzieje się **poza Databricks**, zanim zaczniemy pisać kod pipeline'u.

### Co należy do SETUP

| Krok | Opis | Status |
|---|---|---|
| Repozytorium GitHub | Prywatne repo `order-events-platform` | ✅ Gotowe |
| Struktura plików lokalnie | `notebooks/`, `dlt/`, `databricks.yml`, pliki `.md` | ✅ Gotowe |
| Git Folders w Databricks | Podłączenie repo do Databricks Workspace | ✅ Gotowe |
| Wgranie danych źródłowych | Pliki `batch_*.json` i `products.csv` do Unity Catalog Volume | ✅ Gotowe |

### Workflow Deweloperski

```
VS Code (lokalnie)
    │
    │  edytujesz .py i .yml
    │
    ▼
git push → GitHub
    │
    │  Databricks Git Folders (odśwież)
    ▼
Databricks — uruchamiasz notebooki i Job
```

### Struktura Repozytorium

```
order-events-platform/
├── notebooks/
│   ├── 01_prepare_bronze.py
│   ├── 02_build_silver_and_gold.py
│   └── 03_quality_check.py
├── dlt/
│   └── dlt_design_example.py
├── databricks.yml
├── implementation_plan.md
├── engineering_notes.md
└── self_review.md
```

---

## 📊 Analiza Danych Wejściowych — Co Wiemy

Zanim przejdę do planu, wypiszę kluczowe obserwacje z analizy plików danych, bo one determinują **każdą** decyzję architektoniczną.

### Mapa eventów przez batche

| order_id | batch_001 (seq) | batch_002 (seq) | batch_003 (seq) | Wyzwanie |
 |---|---|---|---|---|
| O-101 | E-1001 (seq=1) | E-2001 (seq=2) | **E-3001 (seq=0)** | ⚠️ Starszy event przychodzi PÓŹNIEJ |
| O-103 | E-1002 (seq=1) | — | — | Brak aktualizacji |
| O-105 | E-1003 (seq=1) | E-2002 (seq=2, CANCELLED) | — | Zmiana statusu |
| O-106 | E-1004 (seq=1) | — | — | Brak aktualizacji |
| O-108 | — | E-2003 (seq=1) | E-3002 (seq=2, CANCELLED) | Zmiana statusu |
| O-109 | — | **E-2004 (P-404)** | — | ⚠️ Produkt nie istnieje w katalogu |
| O-110 | — | — | E-3003 (seq=1) + E-3004 (seq=2) + **`source_priority`** | ⚠️ Nowa kolumna + duplikat w batchu |
| O-111 | — | — | **E-3005 (qty=-1)** | ⚠️ Błąd jakości danych |

### Kluczowe wyzwania zidentyfikowane w danych

1. **Late-arriving event** — E-3001 dla O-101 ma `event_sequence=0`, mimo że nowszy event (seq=2) już jest w Silver
2. **Brakujący produkt** — P-404 nie istnieje w `products.csv`
3. **Schema evolution** — `batch_003` wprowadza pole `source_priority`, którego nie ma w schemacie Bronze
4. **Invalid quality record** — O-111 ma `quantity=-1`
5. **Duplikat w jednym batchu** — O-110 pojawia się dwukrotnie w `batch_003`
6. **Test idempotencji** — `batch_002` uruchamiany dwukrotnie nie może zdublować danych

---

## 🎯 Architektura Ogólna — Medallion Architecture

```
JSON Files (Volume/Workspace)
        │
        ▼
┌─────────────────────┐
│     BRONZE          │  ← Raw events, immutable, schema enforced
│ bronze_order_events │    grain: 1 rekord = 1 event (event_id)
└─────────────────────┘
        │
        ▼  Window Function + MERGE
┌──────────────────────────────────────────┐
│              SILVER                      │
│  silver_orders_current                   │  grain: 1 rekord = 1 order_id (najnowszy)
│  silver_order_items_current              │  grain: 1 rekord = order_id + product_id
└──────────────────────────────────────────┘
        │
        ▼  GROUP BY + filter PAID
┌────────────────────────────────────────────┐
│                GOLD                        │
│  gold_daily_sales       (dzień)            │
│  gold_category_sales    (dzień + kategoria)│
└────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────┐
│   QUALITY CHECK      │  ← Quality gate → może zatrzymać run
└──────────────────────┘
```

### Przepływ Workflow (DAG)

```
prepare_bronze  ──→  build_silver_and_gold  ──→  quality_check
   (Task 1)              (Task 2)                   (Task 3)
```

---

## ETAP 0 — Przygotowanie Środowiska

### 0.1 Struktura Unity Catalog

```
<catalog_name>/
└── de_course_module_03_project/          ← schema (= database)
    ├── bronze_order_events               ← Delta table
    ├── silver_orders_current             ← Delta table
    ├── silver_order_items_current        ← Delta table
    ├── gold_daily_sales                  ← Delta table
    └── gold_category_sales              ← Delta table
```

**Decyzja: Unity Catalog Volume vs Workspace Files**
- Wybieram **Unity Catalog Volume** — daje pełną integrację z lineage, access control przez UC oraz jest zgodny z kierunkiem rozwoju Databricks
- Pliki wgrane z podfolderami `data/orders/` i `data/reference/`
- Ścieżka bazowa: `/Volumes/workspace/de_course_module_03_project/raw_data/data`

### 0.2 Parametry Globalne (widgety)

| Parametr | Wartość | Opis |
|---|---|---|
| `catalog_name` | `workspace` | Unity Catalog |
| `schema_name` | `de_course_module_03_project` | Schema |
| `data_base_path` | `/Volumes/workspace/de_course_module_03_project/raw_data/data` | Katalog z plikami |
| `batch_id` | `batch_001` | Który batch przetwarzamy |
| `run_id` | `manual_run` | ID uruchomienia (do audytu) |

**Ścieżki plików:**

| Plik | Ścieżka |
|---|---|
| Orders batch | `{data_base_path}/orders/{batch_id}_orders.json` |
| Products | `{data_base_path}/reference/products.csv` |

### 0.3 Kolejność Testowania

| Krok | Batch | Cel |
|---|---|---|
| 1 | `batch_001` | Baseline — 4 nowe zamówienia |
| 2 | `batch_002` | Aktualizacje + nowe + brakujący produkt |
| 3 | `batch_002` ponownie | Test **idempotencji** |
| 4 | `batch_003` | Late-arriving event, schema evolution, quality gate fail |

---

## ETAP 1 — Notebook `01_prepare_bronze.py` (Bronze Layer)

### 1.1 Cel i Odpowiedzialność

Bronze to **vault surowych danych** — raz zapisane eventy nigdy nie są modyfikowane. Każde uruchomienie dokłada nowe wiersze (lub pomija duplikaty), nigdy nie nadpisuje.

### 1.2 Jawny Schema dla JSON

**Dlaczego jawny schema?**
Bez jawnego schematu Spark inferuje typy przez full scan pliku — wolno i ryzykowne przy pustych polach. Z jawnym schematem:
- `event_ts` parsowany jako `StringType` → konwertowany do `TimestampType` przez `F.to_timestamp()`
- Pola spoza schematu (np. `source_priority` z batch_003) → domyślnie ignorowane (**schema enforcement**)
- To jest kontrolowana decyzja: Bronze ignoruje `source_priority` teraz, ale można ją jawnie dodać do schematu w ramach **schema evolution**

**Schemat items (nested):**

```
items: ArrayType(
    StructType([
        product_id: StringType
        quantity:   IntegerType
        unit_price: DoubleType
    ])
)
```

### 1.3 Kolumny Techniczne Dodawane przy Ingeście

| Kolumna | Wartość | Cel |
|---|---|---|
| `ingested_at` | `current_timestamp()` | Audit — kiedy event trafił do Bronze |
| `source_file` | `input_file_name()` | Lineage — z jakiego pliku pochodzi |
| `run_id` | widget `run_id` | Powiązanie z konkretnym uruchomieniem Joba |
| `batch_id` | z JSON (już w danych) + widget | Filtrowanie w dalszych etapach |

### 1.4 Idempotencja — Kluczowy Mechanizm

**Problem:** Uruchomienie batch_002 dwa razy nie może zdublować 4 eventów.

**Rozwiązanie — MERGE po `event_id`:**

```
MERGE INTO bronze_order_events AS target
USING new_events AS source
  ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *
```

- `event_id` jest naturalnym kluczem biznesowym eventu (np. `E-2001`)
- `WHEN NOT MATCHED THEN INSERT *` — wstawia tylko te eventy, których jeszcze nie ma
- Brak `WHEN MATCHED THEN UPDATE` — celowo: Bronze jest **append-only** dla nowych eventów

**Dlaczego MERGE, a nie INSERT WHERE NOT IN?**
MERGE jest atomiczny — przy równoległych uruchomieniach nie wystąpi race condition.

### 1.5 Obsługa Schema Evolution (`source_priority` z batch_003)

- **Opcja A (wybrana):** Schema Bronze jest ścisła — `source_priority` jest ignorowane przy wczytaniu (schema enforcement). Jeśli chcemy go zachować, explicite rozszerzamy schema i dokumentujemy decyzję w `engineering_notes.md`
- **Opcja B:** `mergeSchema = True` w opcjach zapisu — elastyczne, ale wymaga jawnego zatwierdzenia

W projekcie pokazujemy **oba podejścia**: najpierw enforcement (batch_003 ładuje się bez `source_priority`), potem świadome dodanie kolumny przez `ALTER TABLE`.

### 1.6 Wynik — Schemat Tabeli Bronze

```
bronze_order_events:
├── event_id        (String, NOT NULL) ← klucz idempotencji
├── batch_id        (String)
├── source_system   (String)
├── order_id        (String)
├── customer_id     (String)
├── event_ts        (Timestamp)
├── event_sequence  (Integer)
├── status          (String)
├── currency        (String)
├── items           (Array<Struct>)
├── ingested_at     (Timestamp)  ← techniczne
├── source_file     (String)     ← techniczne
└── run_id          (String)     ← techniczne
```

> **Grain Bronze:** 1 wiersz = 1 event biznesowy (identyfikowany przez `event_id`).
> Tabela zawiera **wszystkie** eventy ze wszystkich batchy, w tym starsze wersje tego samego zamówienia.

---

## ETAP 2 — Notebook `02_build_silver_and_gold.py` (Silver + Gold)

### 2.1 SILVER — Część A: `silver_orders_current`

#### 2.1.1 Strategia: Wybór Najnowszego Eventu

**Problem:** Dla O-101 mamy w Bronze 3 eventy (seq=1, seq=2, seq=0). Silver ma zawierać TYLKO aktualny stan.

**Mechanizm — Window Function:**

```
Window:
  PARTITION BY order_id
  ORDER BY event_ts DESC, event_sequence DESC, event_id DESC
  → ROW_NUMBER() = 1
```

**Deterministyczny tie-breaker (trzypoziomowy):**

| Priorytet | Kolumna | Kierunek | Uzasadnienie |
|---|---|---|---|
| 1 | `event_ts` | DESC | Późniejszy timestamp = nowszy event |
| 2 | `event_sequence` | DESC | Wyższy sequence = nowszy (tie-breaker gdy ts równe) |
| 3 | `event_id` | DESC | Ostatni fallback — lexicographic |

**Wynik:** Dla O-101 wygrywający event to E-2001 (seq=2). E-3001 (seq=0) NIE może go nadpisać. ✅

**Gdzie pojawia się shuffle?**
Window Function z `PARTITION BY` wymusza shuffle po `order_id` — Spark musi zebrać wszystkie eventy dla jednego `order_id` na jednym executorze. To jest jedno z miejsc na `explain()`.

#### 2.1.2 Deduplikacja Source PRZED MERGE

**Problem:** `batch_003` zawiera dwa eventy dla O-110 (E-3003 i E-3004). MERGE z duplikatem source po tym samym kluczu rzuci błąd lub da niedeterministyczny wynik.

**Rozwiązanie:** Window Function na `batch_df` (nie na całym Bronze) przed MERGE → po Window dostajemy zdeduplikowany DataFrame z jednym wierszem per `order_id`.

#### 2.1.3 MERGE do `silver_orders_current`

```
MERGE INTO silver_orders_current AS target
USING deduplicated_latest_events AS source
  ON target.order_id = source.order_id
WHEN MATCHED AND source.event_sequence > target.event_sequence THEN UPDATE SET *
WHEN MATCHED AND source.event_ts > target.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Kluczowy guard clause:** `source.event_sequence > target.event_sequence`
- E-3001 (seq=0) przychodzi gdy target ma już seq=2
- Warunek `0 > 2` = `False` → UPDATE nie następuje → late-arriving event jest bezpiecznie odrzucany ✅

**Dlaczego MERGE, a nie overwrite?**
- MERGE jest atomiczny na poziomie wiersza
- Zachowuje wiersze bez nowych eventów w bieżącym batchu (O-103, O-106 nie są w batch_002 → zostają nienaruszone)
- Overwrite usunąłby te wiersze bezpowrotnie

> **Grain Silver Orders:** 1 wiersz = 1 `order_id` = **aktualny stan zamówienia**

#### 2.1.4 SILVER — Część B: `silver_order_items_current`

**Transformacje krok po kroku:**

| Krok | Operacja | Wynik |
|---|---|---|
| 1 | `explode(items)` | Każda pozycja zamówienia = osobny wiersz |
| 2 | `line_revenue = quantity * unit_price` | Kolumna kalkulowana |
| 3 | LEFT JOIN z products.csv (broadcast) | Dołączenie nazwy i kategorii produktu |
| 4 | Flaga `product_missing` | `True` gdy P-404 (brak w katalogu) |
| 5 | Flaga `is_valid_item` | `quantity > 0 AND unit_price >= 0 AND NOT product_missing` |

**Szczegóły Broadcast Join:**

```
products_df = spark.read.csv(products_path)   # ~5 wierszy
# Spark automatycznie wybierze BroadcastHashJoin dla tak małej tabeli
# Można wymusić explicite: F.broadcast(products_df)
# Brak shuffle po stronie products!
```

W `explain()` zobaczymy `BroadcastHashJoin` zamiast `SortMergeJoin`.

**Strategia zapisu Silver Items:**
Tabela jest **przebudowywana (overwrite)** przy każdym uruchomieniu — nie używamy MERGE dla items, bo po `explode` naturalnym kluczem byłoby `(order_id, product_id)`, co jest niejednoznaczne (ten sam produkt może wystąpić dwa razy w jednym zamówieniu). Silver Items zawsze odzwierciedla aktualny stan `silver_orders_current`.

> **Grain Silver Items:** 1 wiersz = 1 pozycja zamówienia z aktualnego eventu (order_id + pozycja z tablicy items)

### 2.2 GOLD — Tabele Analityczne

#### 2.2.1 `gold_daily_sales`

**Źródło:** `silver_order_items_current` JOIN `silver_orders_current` (dla statusu)

**Filtr:** `status = 'PAID'` AND `is_valid_item = True`

**Agregacja:**

```
GROUP BY DATE(event_ts)

Metryki:
  order_count    = COUNT(DISTINCT order_id)
  total_items    = COUNT(*)
  total_revenue  = SUM(line_revenue)
```

> **Grain:** 1 wiersz = 1 dzień sprzedaży

#### 2.2.2 `gold_category_sales`

**Źródło:** Jak wyżej + kolumna `category` z products join

**Agregacja:**

```
GROUP BY DATE(event_ts), category

Metryki:
  order_count       = COUNT(DISTINCT order_id)
  item_count        = COUNT(*)
  category_revenue  = SUM(line_revenue)
```

> **Grain:** 1 wiersz = 1 dzień + 1 kategoria produktu

**Uwaga:** Zamówienia z `product_missing = True` (O-109, P-404) mają `category = NULL`.
Decyzja: włączyć z kategorią `"Unknown"` lub wykluczyć — dokumentujemy w engineering notes.

#### 2.2.3 Strategia Zapisu Gold

Gold używa **overwrite** (nie MERGE) — agregaty są zawsze liczone od nowa z Silver.
Overwrite jest atomiczny w Delta i gwarantuje spójność przy ponownych uruchomieniach.

### 2.3 Spark Explain — Podsumowanie

| Operacja | Widoczne w Explain | Shuffle? |
|---|---|---|
| Window Function (dedup) | `WindowExec` → `Exchange hashpartitioning(order_id)` | ✅ TAK |
| Broadcast Join (products) | `BroadcastHashJoin` | ❌ NIE (po stronie products) |
| Agregacja Gold | `HashAggregate` + `Exchange hashpartitioning(date)` | ✅ TAK |

**Decyzja o cache:**
W tym projekcie `cache()` jest opcjonalny. Uzasadniony przypadek użycia: `silver_items_df.cache()` przed budową dwóch tabel Gold — DataFrame jest wtedy obliczany raz i czytany dwa razy. Poza tym przypadkiem cache nie przynosi korzyści (każdy DF używany jednokrotnie).

---

## ETAP 3 — Notebook `03_quality_check.py` (Quality Gate)

### 3.1 Metryki Jakości

**Źródło:** `silver_order_items_current`

| Metryka | Warunek | Przypadek w danych |
|---|---|---|
| `invalid_quantity_count` | `quantity <= 0` | O-111, E-3005 (qty=-1) |
| `invalid_price_count` | `unit_price < 0` | Brak w danych testowych |
| `missing_product_count` | `product_missing == True` | O-109, P-404 |

### 3.2 Progi i Logika Gate

| Metryka | Widget | Domyślna wartość | Akcja |
|---|---|---|---|
| `invalid_quantity_count` | `max_invalid_items` | `0` | ❌ `raise Exception` → Job FAILS |
| `invalid_price_count` | `max_invalid_items` | `0` | ❌ `raise Exception` → Job FAILS |
| `missing_product_count` | `max_missing_products` | `10` | ⚠️ Warning (P-404 to oczekiwany przypadek) |

### 3.3 Kontrolowany Failed Run (batch_003)

**Scenariusz FAIL:**
O-111 ma `quantity=-1` → `invalid_quantity_count = 1` → przekracza próg `0`
→ `raise Exception("Quality gate failed: 1 invalid quantity records found")` → **Job FAILED** ✅

**Scenariusz Successful Run po poprawce:**

- **Opcja A (demo):** Podnieść próg `max_invalid_items` z `0` do `1` — Job PASSES
- **Opcja B (produkcja):** Dodać quarantine table, wykluczyć invalid items z Silver → Job PASSES

Dokumentujemy Opcję A jako quick fix do demonstracji, opisujemy Opcję B jako rozwiązanie produkcyjne.

### 3.4 Opcjonalne: Quality Summary Table

Zapisanie metryk do `gold_data_quality_summary` jako osobna tabela — element opcjonalny, ale wzmacniający projekt.

---

## ETAP 4 — Databricks Workflow i Asset Bundles

### 4.1 Struktura `databricks.yml`

```
bundle: module_03_order_events_project
target: dev

job: order_events_platform
  timeout: 1800s
  parameters: catalog_name, schema_name, data_base_path, batch_id, run_id

  tasks:
    [1] prepare_bronze          ← notebook 01
    [2] build_silver_and_gold   ← notebook 02, depends_on: [1]
    [3] quality_check           ← notebook 03, depends_on: [2], max_retries: 0
```

### 4.2 Uzasadnienie Konfiguracji

**Dlaczego `max_retries: 0` dla quality_check?**
Quality gate ma działać jako hard stop — retry przy błędzie jakości danych nie ma sensu, bo dane się nie zmienią między próbami.

**Dlaczego retry dla prepare_bronze?**
Potencjalne transient failures (sieć, storage) → 1-2 retry są uzasadnione dla warstwy ingestii.

**Parametry Joba:**
Pozwalają uruchamiać ten sam workflow dla różnych batchy bez edycji kodu — kluczowe dla operacyjności.

### 4.3 Walidacja Bundle

```bash
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run order_events_platform --target dev
```

---

## ETAP 5 — Delta / Unity Catalog Showcases

### 5.1 DESCRIBE HISTORY

Pokażemy historię dla `silver_orders_current` — najbardziej interesująca tabela, bo zawiera operacje MERGE:

```sql
DESCRIBE HISTORY <catalog>.<schema>.silver_orders_current;
-- Widoczne: MERGE operacje z timestampami, user, operationParameters
```

### 5.2 Time Travel

**Scenariusz:** Przed i po MERGE dla batch_002 na O-105 (zmiana PAID → CANCELLED):

```sql
-- Stan przed batch_002 (wersja po batch_001):
SELECT * FROM silver_orders_current VERSION AS OF 1 WHERE order_id = 'O-105';
-- Wynik: status = PAID

-- Stan aktualny (po batch_002):
SELECT * FROM silver_orders_current WHERE order_id = 'O-105';
-- Wynik: status = CANCELLED
```

### 5.3 Schema Enforcement i Evolution

**Enforcement (batch_003 bez source_priority):**
- Wczytanie z jawnym schematem → `source_priority` jest pomijane
- Demonstracja: `bronze_order_events` NIE ma kolumny `source_priority`

**Evolution (świadome dodanie kolumny):**

```sql
ALTER TABLE bronze_order_events ADD COLUMN source_priority STRING;
-- lub przez Spark:
-- spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

---

## ETAP 6 — DLT i API (Dokumentacja)

### 6.1 DLT Alternative

| Komponent | Odpowiednik w DLT | Uwagi |
|---|---|---|
| Bronze ingest | `@dlt.table` + Auto Loader `readStream.format("cloudFiles")` | Streaming, automatyczny |
| Silver Items | `@dlt.table` + `@dlt.expect_or_fail("valid_quantity", "quantity > 0")` | Wbudowany quality gate |
| Gold | `@dlt.table` jako materializowany widok | Deklaratywne agregacje |

**Ograniczenie DLT:** MERGE z custom guard clause (starszy event nie nadpisuje nowszego) jest trudniejszy do wyrażenia w deklaratywnym API DLT niż w imperatywnym PySpark.

### 6.2 Jobs API

```bash
POST /api/2.1/jobs/run-now
Authorization: Bearer <TOKEN_FROM_SECRET_SCOPE>

Body:
{
  "job_id": 123456789,
  "notebook_params": {
    "catalog_name": "workspace",
    "schema_name": "de_course_module_03_project",
    "batch_id": "batch_002",
    "run_id": "api_run_001"
  }
}
```

**Token:** Przechowywany w **Databricks Secret Scope** — nigdy w kodzie ani w repozytorium.

---

## ETAP 7 — Engineering Notes i Self-Review

### 7.1 Grain Table — Podsumowanie

| Tabela | Warstwa | Grain | Klucz | Uwagi |
|---|---|---|---|---|
| `bronze_order_events` | Bronze | 1 wiersz = 1 event | `event_id` | Append-only, immutable |
| `silver_orders_current` | Silver | 1 wiersz = 1 zamówienie | `order_id` | Aktualny stan, MERGE |
| `silver_order_items_current` | Silver | 1 wiersz = 1 pozycja aktualnego zamówienia | `(order_id, position)` | Overwrite z Silver Orders |
| `gold_daily_sales` | Gold | 1 wiersz = 1 dzień | `order_date` | Tylko PAID + is_valid |
| `gold_category_sales` | Gold | 1 wiersz = dzień + kategoria | `(order_date, category)` | Tylko PAID + is_valid |

---

## 🔑 Kluczowe Decyzje Architektoniczne — Podsumowanie

| Decyzja | Wybór | Uzasadnienie |
|---|---|---|
| Przechowywanie plików | Unity Catalog Volume | Integracja z lineage, access control |
| Bronze idempotencja | MERGE po `event_id` | Atomiczne, bezpieczne przy retry |
| Tie-breaker dla dedup | `event_ts → event_sequence → event_id` | Deterministyczny, 3-poziomowy |
| Guard clause w MERGE | `source.event_sequence > target.event_sequence` | Chroni przed late-arriving events |
| Products join | Broadcast join | ~5 wierszy → zawsze broadcast |
| Silver Items strategy | Overwrite (nie MERGE) | Grain po explode jest niejednoznaczny |
| Gold strategy | Overwrite z Silver | Agregaty, single source of truth |
| Cache | Opcjonalny dla Silver Items | Używany przez dwie tabele Gold |
| Quality gate | `raise Exception` | Hard stop, bez retry |
| Schema evolution | Jawna decyzja + `ALTER TABLE` | Kontrolowane, dokumentowane |

---

## ✅ Kolejność Implementacji (Rekomendowana)

```
 1. Środowisko      → wgranie danych do Volume, CREATE SCHEMA
 2. Bronze          → notebook 01 + test batch_001
 3. Bronze test     → idempotencja batch_002 × 2
 4. Silver Orders   → Window + MERGE + test late-arriving (batch_003)
 5. Silver Items    → explode + broadcast join + flagi jakości
 6. Gold            → agregacje + overwrite
 7. Quality Check   → notebook 03 + failed/successful run
 8. databricks.yml  → bundle + walidacja
 9. Explain         → plany dla Window, join, agregacji
10. Delta showcases → HISTORY + Time Travel
11. DLT + API       → dokumentacja
12. Engineering Notes + Self-Review → finalizacja
```

---

## 📋 Checklist Przed Oddaniem

### Architektura
- [ ] Bronze, Silver i Gold gotowe
- [ ] Grain każdej tabeli opisany
- [ ] Gold liczony z Silver (nie bezpośrednio z Bronze)
- [ ] Raw eventy zachowane w Bronze

### PySpark
- [ ] DataFrame API użyte wszędzie
- [ ] Window Function do wyboru aktualnego eventu
- [ ] `explode` do rozbicia items
- [ ] `line_revenue` obliczone
- [ ] Brak Python UDF tam, gdzie wystarczą funkcje Spark SQL

### Incremental Processing
- [ ] MERGE do aktualizacji Silver
- [ ] Guard clause chroni przed starszym eventem
- [ ] Deduplikacja przed MERGE
- [ ] Idempotencja zweryfikowana (batch_002 × 2)

### Databricks / Delta
- [ ] Pełne nazwy tabel (`catalog.schema.table`)
- [ ] Tabele managed Delta
- [ ] `DESCRIBE HISTORY` pokazane
- [ ] Time Travel pokazane
- [ ] Schema enforcement i evolution opisane

### Spark Internals
- [ ] `explain` dla Window Function
- [ ] `explain` dla joinu z products
- [ ] `explain` dla agregacji Gold
- [ ] Shuffle wskazany
- [ ] Broadcast join wyjaśniony

### Workflow
- [ ] Workflow z 3 taskami
- [ ] Parametry Joba
- [ ] Quality gate działa
- [ ] Failed run pokazany (batch_003, qty=-1)
- [ ] Successful run pokazany
- [ ] `databricks.yml` gotowy

### Dokumentacja
- [ ] `engineering_notes.md` uzupełniony
- [ ] Brak tokenów i sekretów w repozytorium
- [ ] Decyzje produkcyjne opisane
- [ ] `self_review.md` wypełniony
