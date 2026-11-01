# Engineering Notes

## Jak Uruchomić Projekt

**Dane.** Unity Catalog Volume (nie Workspace Files — lineage i access control przez UC):

```
/Volumes/workspace/de_course_module_03_project/raw_data/data
├── orders/batch_001_orders.json, batch_002_orders.json, batch_003_orders.json
└── reference/products.csv
```

**Parametry** (widgety notebooków = parametry Joba):

| Parametr | Domyślnie | Opis |
|---|---|---|
| `catalog_name` | `workspace` | Unity Catalog (pusty = `current_catalog()`) |
| `schema_name` | `de_course_module_03_project` | Schema |
| `data_base_path` | ścieżka Volume powyżej | Katalog z plikami |
| `batch_id` | `batch_001` | Batch do przetworzenia |
| `run_id` | `bundle_dev_run` | Audyt — zapisywany w Bronze |
| `max_invalid_items` | `0` | Próg quality gate dla `quantity <= 0` i `unit_price < 0` |
| `max_missing_products` | `10` | Próg dla brakujących produktów |

**Kolejność batchy:**

1. `batch_001` — baseline (4 nowe zamówienia)
2. `batch_002` — aktualizacje, nowe zamówienia, brakujący produkt P-404
3. `batch_002` ponownie — test idempotencji
4. `batch_003` — late-arriving event, duplikat w batchu, `source_priority`, `qty=-1` (quality gate FAIL)

**Ręcznie (Run all):** notebooki `01` → `02` → `03` z ustawionym `batch_id`
(Git Folder w Databricks: `git push`, potem Pull).

**Job / Workflow** (Databricks Asset Bundles, wymaga Databricks CLI i zalogowanego profilu):

```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run order_events_platform --target dev --params batch_id=batch_003,run_id=bundle_failed_run
databricks bundle run order_events_platform --target dev --params batch_id=batch_003,max_invalid_items=1,run_id=bundle_success_run
```

Job ma 3 taski: `prepare_bronze` (retry 2×) → `build_silver_and_gold` → `quality_check` (`max_retries: 0`),
timeout 1800 s, `max_concurrent_runs: 1`. Compute: Serverless.

**Failed i successful run.** `batch_003` zawiera E-3005 (O-111, `qty=-1`), więc `quality_check` kończy się:
`Quality gate failed: 1 invalid quantity records found (max_invalid_items=0)`. Successful run to ten sam batch
z `max_invalid_items=1` — to obejście na potrzeby demonstracji (patrz „Co zmieniłbym w produkcji").
Rekord O-111 zostaje w Silver, więc każdy kolejny run z progiem `0` też kończy się FAILED.

**Notebook `04_delta_showcases`** (poza Jobem, uruchamiany ręcznie): `DESCRIBE HISTORY`, Time Travel,
schema enforcement i evolution.

## Tabele i Grain

| Tabela | Warstwa | Grain | Do Czego Służy |
|---|---|---|---|
| `bronze_order_events` | Bronze | 1 wiersz = 1 event (`event_id`) | Vault surowych danych, immutable |
| `silver_orders_current` | Silver | 1 wiersz = 1 zamówienie (`order_id`) | Aktualny stan zamówienia |
| `silver_order_items_current` | Silver | 1 wiersz = 1 pozycja aktualnego zamówienia | Pozycje po explode, z flagami jakości |
| `gold_daily_sales` | Gold | 1 wiersz = 1 dzień | Sprzedaż dzienna (tylko PAID) |
| `gold_category_sales` | Gold | 1 wiersz = dzień + kategoria | Sprzedaż wg kategorii (tylko PAID) |

## Decyzje Techniczne

### MERGE

| Tabela | Klucz | Strategia |
|---|---|---|
| `bronze_order_events` | `event_id` | `WHEN NOT MATCHED THEN INSERT *` — bez UPDATE, Bronze jest append-only |
| `silver_orders_current` | `order_id` | MERGE z guard clause (niżej) |
| `silver_order_items_current`, Gold | — | overwrite, nie MERGE |

- **Idempotencja Bronze:** ponowne wczytanie tego samego batcha nie dodaje wierszy (`batch_002` × 2 → nadal 8 eventów).
- **Guard clause w Silver:** jedno porównanie krotki, w tej samej kolejności co tie-breaker Window:
  `WHEN MATCHED AND struct(source.event_ts, source.event_sequence, source.event_id) >
  struct(target.event_ts, target.event_sequence, target.event_id) THEN UPDATE`. Late-arriving E-3001 (O-101, seq=0)
  nie nadpisał E-2001 (seq=2): po MERGE z `batch_003` O-101 nie zmienił się. Ten sam event wczytany ponownie jest
  równy, a nie większy, więc MERGE niczego nie aktualizuje (metryki: `numTargetRowsUpdated = 0`).
- **Duplikaty w source:** przed MERGE Window Function (`row_number` po `order_id`, sortowanie
  `event_ts DESC, event_sequence DESC, event_id DESC`) zostawia 1 event na zamówienie. W `batch_003`
  O-110 ma E-3003 (seq=1) i E-3004 (seq=2) — w Silver jest E-3004 i MERGE nie zgłasza błędu.
- **Dlaczego overwrite dla items i Gold:** po `explode` klucz `(order_id, product_id)` jest niejednoznaczny
  (ten sam produkt może wystąpić dwa razy), a Gold to agregaty liczone od nowa z Silver.
  Overwrite Delta jest atomowy.
- **Zmiana guard clause i test.** Pierwotnie były dwa warunki połączone `OR` (`event_sequence >` albo `event_ts >`).
  Zastąpiłem je jednym porównaniem krotki, żeby MERGE i Window zawsze zgadzały się, który event jest nowszy
  (przy `OR` event z wyższym `event_sequence`, ale starszym `event_ts` nadpisałby wiersz). Na danych kursu oba
  warianty dają ten sam wynik. Po zmianie przeszedłem cały Job w osobnym schemacie
  (`batch_001` → `batch_002` → `batch_002` → `batch_003` fail → `batch_003` z progiem 1) i porównałem wszystkie
  tabele wiersz po wierszu z wersją sprzed zmiany: identyczne. Przypadki konfliktu `event_ts` / `event_sequence`
  sprawdziłem tylko na literałach w Spark SQL (`SELECT struct(...) > struct(...)`), nie na danych w tabeli:
  wyższy `event_sequence` przy starszym `event_ts` daje `false`, a nowszy `event_ts` przy niższym
  `event_sequence` daje `true`.

### Broadcast Join

- Broadcastuję `products.csv` (kilka wierszy) przez `F.broadcast(products_df)` w LEFT JOIN do pozycji zamówień.
  Mała tabela jest kopiowana do wykonawców, więc duża strona (pozycje zamówień) nie jest shufflowana po `product_id`.
- W `explain`: `PhotonBroadcastHashJoin LeftOuter` (zamiast `SortMergeJoin`) — patrz „Spark Explain".

### Cache

Nie używam `cache()`. Serverless nie wspiera `PERSIST TABLE`, więc oba Gold czytają zapisaną tabelę
`silver_order_items_current`. Na klasycznym klastrze `cache()` miałoby sens dla Silver Items
(czytanego dwa razy: `gold_daily_sales` i `gold_category_sales`).

## Jakość Danych

Błędne rekordy są **oznaczane i mierzone**, a nie usuwane: `silver_order_items_current` ma flagi
`product_missing` i `is_valid_item` (`quantity > 0 AND unit_price >= 0 AND NOT product_missing`).

| Metryka (notebook 03) | Warunek | Próg (widget) | Efekt przekroczenia |
|---|---|---|---|
| `invalid_quantity_count` | `quantity <= 0` | `max_invalid_items` = 0 | **FAIL** (`raise Exception`) |
| `invalid_price_count` | `unit_price < 0` | `max_invalid_items` = 0 | **FAIL** |
| `missing_product_count` | `product_missing = true` | `max_missing_products` = 10 | FAIL powyżej progu |

- **Co zatrzymuje run:** `raise Exception("Quality gate failed: ...")` w tasku `quality_check`. Job zatrzymuje się
  kontrolowanie, a `max_retries: 0` — ponowienie nie zmieni danych.
- **Co jest tylko ostrzeżeniem:** brakujące produkty w liczbie ≤ progu (notebook wypisuje `WARNING`).
- **Brakujący produkt P-404 (O-109):** LEFT JOIN zostawia wiersz z `product_name = NULL`, więc `product_missing = true`
  i `is_valid_item = false`. Nie zatrzymuje runu (1 ≤ 10), ale jest wykluczony z Gold (filtr `is_valid_item`).
  W `gold_category_sales` jest więc `coalesce(category, 'Unknown')`, ale w obecnych danych nic go nie używa —
  rekordy bez kategorii odpadają wcześniej na filtrze.
- **Stan po `batch_003`:** w Silver są dokładnie dwie niepoprawne pozycje — O-109/P-404 (brak produktu) i O-111/P-02
  (`qty=-1`). O-111 ma status PAID, ale `is_valid_item = false`, więc nie trafia do Gold.
- **Ograniczenie:** notebook 03 liczy metryki na całym Silver, nie tylko na bieżącym batchu, więc zły rekord
  blokuje także kolejne runy z progiem 0.

## Delta / Unity Catalog

**Pełne nazwy tabel** (managed Delta, katalog `workspace`, schema `de_course_module_03_project`):
`workspace.de_course_module_03_project.bronze_order_events`, `silver_orders_current`, `silver_order_items_current`,
`gold_daily_sales`, `gold_category_sales`. Stan po `batch_003`: 13 / 8 / 9 / 4 / 4 wierszy.

**DESCRIBE HISTORY** (`silver_orders_current`), przebieg `batch_001` → `batch_002` → `batch_002` → `batch_003` →
`batch_003` (Job, 5 runów):

| Wersja | Operacja | Skąd |
|---|---|---|
| v0 | `CREATE OR REPLACE TABLE AS SELECT` | `batch_001` (4 wiersze) |
| v1 | `MERGE` | `batch_002` (2 wstawione, 2 zaktualizowane) |
| v2 | `OPTIMIZE` | w notebookach nie ma tego polecenia, więc najpewniej wykonała je platforma automatycznie |
| v3 | `MERGE` | `batch_002` ponownie (0 wstawionych, 0 zaktualizowanych) |
| v4 | `MERGE` | `batch_003` (2 wstawione, 1 zaktualizowany) |
| v5 | `OPTIMIZE` | jak v2 |
| v6 | `MERGE` | `batch_003` ponownie (0 wstawionych, 0 zaktualizowanych) |
| v7 | `SET TBLPROPERTIES` | retencja Time Travel 30 dni |

Każde uruchomienie MERGE tworzy nową wersję, także gdy nic się nie zmienia (v3, v6). Metryki pochodzą z
`operationMetrics`. W `operationParameters.matchedPredicates` widać warunek guard clause jako porównanie `struct(...)`.

**Time Travel** (notebook 04, O-105 — domyślny widget `order_id`):

| Wersja | status | event_id | event_sequence |
|---|---|---|---|
| `VERSION AS OF 0` (po `batch_001`) | PAID | E-1003 | 1 |
| `VERSION AS OF 1` (po MERGE z `batch_002`) | CANCELLED | E-2002 | 2 |

Notebook sam wyznacza te wersje z historii (pierwsza wersja, w której status zmienił się na `CANCELLED`).
Między v3 a v4 (`batch_003`) zmieniły się: O-108 PAID → CANCELLED (E-2003 → E-3002) oraz doszły O-110 i O-111.
O-101 jest przy E-2001 zarówno w v3, jak i w v6 — late-arriving E-3001 został odrzucony przez guard clause.

**Ograniczenie Time Travel.** `DESCRIBE HISTORY` pokazuje wersje (log: 30 dni), ale `VERSION AS OF` działa tylko
w oknie `delta.deletedFileRetentionDuration` (domyślnie 7 dni, do czasu `VACUUM`). W pierwszym schemacie wersje
sprzed 10 dni zwracały `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION`; po ustawieniu
30 dni (`ALTER TABLE ... SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')`) były
znów czytelne, bo `VACUUM` nigdy nie był uruchamiany. Obecne tabele zostały odbudowane od zera, a ta sama
retencja jest ustawiona na `silver_orders_current` (v7). Wersje starsze niż 30 dni przestaną być dostępne,
a retencji nie da się przywrócić wstecz, jeśli pliki już usunięto.

**Schema enforcement.** Notebook 01 czyta JSON z jawnym schematem, więc pole `source_priority` z `batch_003`
jest pomijane już przy odczycie. Potwierdzone: `bronze_order_events` nie ma tej kolumny. Dodatkowo notebook 04
pokazuje na tymczasowej tabeli, że zapis z nadmiarową kolumną bez `mergeSchema` Delta odrzuca.

**Schema evolution.** Ten sam zapis z `option("mergeSchema", "true")` dodaje kolumnę (pokazane na tabeli
tymczasowej, usuwanej na końcu notebooka 04). Decyzja w projekcie: `source_priority` zostaje poza Bronze, bo żadna
warstwa jej nie używa. Gdyby była potrzebna: `ALTER TABLE ... ADD COLUMN source_priority STRING` i rozszerzenie
`order_schema` w notebooku 01. `autoMerge` pozostaje wyłączone — evolution ma być świadomą decyzją.

## Spark Explain

Notebook 02 drukuje `explain("formatted")` dla Window, joinu i agregacji Gold. Fragmenty poniżej pochodzą z
`EXPLAIN FORMATTED` uruchomionego jako **odpowiedniki SQL** tych zapytań na SQL warehouse (Serverless, Photon),
a nie z wydruku notebooka — nazwy operatorów są te same, ale szczegóły mogą się nieco różnić.

### Window Function (dedup po `order_id`)

```
PhotonWindow  row_number() OVER (PARTITION BY order_id ORDER BY event_ts DESC, event_sequence DESC, event_id DESC)
+- PhotonSort [order_id ASC, event_ts DESC, event_sequence DESC, event_id DESC]
   +- PhotonScan bronze_order_events   (DictionaryFilters: batch_id = batch_003)
```

Window wymaga, by wszystkie eventy jednego `order_id` trafiły obok siebie, stąd `PhotonSort` po kluczu partycji.
W tym planie **nie ma** węzła `Exchange hashpartitioning(order_id)`. Prawdopodobna przyczyna to mały zbiór danych
(kilkanaście wierszy) i działanie Photon, ale tego nie sprawdzałem np. przez liczbę partycji. Logicznie `PARTITION BY
order_id` wymaga zebrania eventów jednego zamówienia razem, więc na dużych danych oczekiwałbym tu shuffle po
`order_id` — to najkosztowniejsze miejsce pipeline'u.

### Join z products

```
PhotonBroadcastHashJoin LeftOuter
:- PhotonProject  <- PhotonGenerate explode(items) <- PhotonScan silver_orders_current
+- PhotonShuffleExchangeSource <- PhotonShuffleMapStage <- PhotonShuffleExchangeSink
   <- PhotonFilter (product_id IS NOT NULL) <- Scan csv products.csv
```

`PhotonBroadcastHashJoin` zamiast `SortMergeJoin`. Strona z pozycjami zamówień (`explode` z `silver_orders_current`)
wchodzi do joinu bez węzła wymiany; węzły `ShuffleExchange` występują tylko po stronie małej tabeli `products`
(jej odczyt z CSV). Zakładam, że to etap przygotowania danych do broadcastu — nie sprawdzałem tego w dokumentacji.
Zapytanie SQL użyte do planu miało wskazówkę `/*+ BROADCAST(p) */`, a w notebooku jest `F.broadcast(...)`.
Bez wskazówki i dla dużej tabeli `products` oczekiwałbym `SortMergeJoin` z shuffle po `product_id`.

### Agregacja Gold (`gold_daily_sales`)

```
PhotonGroupingAgg (final: count, sum, count distinct order_id)  GROUP BY to_date(event_ts)
+- PhotonGroupingAgg (merge)  <- PhotonGroupingAgg (merge, klucz: data + order_id)
   <- PhotonGroupingAgg (partial) <- PhotonProject <- PhotonScan silver_order_items_current
```

Filtry `status = 'PAID'` i `is_valid_item` są wypchnięte do skanu (`DictionaryFilters`). `COUNT(DISTINCT order_id)`
daje dodatkowy poziom agregacji (data + `order_id`). W planie również brak węzła `Exchange`.

**Wniosek:** shuffle po stronie Window i agregacji jest konieczny logicznie, ale w planach z tego zbioru danych
go nie widać (zob. „Ograniczenia środowiska" poniżej). Broadcast join jest widoczny wprost
(`PhotonBroadcastHashJoin`).

## Ograniczenia Środowiska

Środowisko: Databricks Free Edition, Serverless compute z Photon, Unity Catalog.

| Ograniczenie | Szczegół | Rozwiązanie |
|---|---|---|
| `cache()` nie działa | Serverless nie wspiera `PERSIST TABLE` | Obie tabele Gold czytają zapisaną tabelę Delta |
| `input_file_name()` | Deprecated w Unity Catalog, nie zwraca wartości dla Volumes | `F.col("_metadata.file_path")` jako `source_file` |
| Time Travel poza retencją | `VERSION AS OF` starszych niż `deletedFileRetentionDuration` (7 dni) zwraca błąd, choć `DESCRIBE HISTORY` je pokazuje | Retencja `silver_orders_current` ustawiona na 30 dni (opis w sekcji Delta) |
| Brak `Exchange` w planach | Mały zbiór danych i Photon — plany Window i agregacji nie zawierają węzła `Exchange` | Opisane w sekcji Spark Explain; przyczyny nie weryfikowałem |
| Quality gate po `batch_003` | Notebook 03 liczy metryki na całym Silver, więc O-111 (`qty=-1`) blokuje kolejne runy z progiem 0 | Successful run z `max_invalid_items=1` (obejście demo) — rozwiązanie produkcyjne w „Co zmieniłbym w produkcji" |

## Delta Live Tables (alternatywa, bez deploymentu)

Szkic: `dlt/dlt_design_example.py`. Nie był deployowany ani uruchamiany — to opis alternatywy, zgodnie z wymaganiami.

| Część pipeline'u | Odpowiednik w DLT | Uwagi |
|---|---|---|
| Bronze (notebook 01) | `@dlt.table` + Auto Loader (`cloudFiles`) | Auto Loader sam wykrywa nowe pliki — odpada ręczny `batch_id` |
| `silver_orders_current` | `dlt.apply_changes` (`keys=order_id`, `sequence_by`) | Zastępuje Window + MERGE z guard clause |
| `silver_order_items_current` | `@dlt.table` + expectations | Przeliczany z aktualnego Silver |
| Gold | `@dlt.table` (materializowany widok) | Agregacje deklaratywne |
| Quality gate (notebook 03) | `@dlt.expect*` | Metryki jakości w event logu pipeline'u |

**Przykładowa tabela:** `silver_order_items_dlt` (explode + join z produktami + `line_revenue`).

**Przykładowe expectations:**

```python
@dlt.expect_or_fail("valid_quantity", "quantity > 0")   # zatrzymuje update (jak raise Exception)
@dlt.expect_or_fail("valid_price", "unit_price >= 0")
@dlt.expect("product_exists", "product_name IS NOT NULL")  # tylko metryka (jak WARNING dla P-404)
```

**Co trudniej w DLT:** `apply_changes` ma wbudowaną logikę „nowszy wygrywa" przez `sequence_by` (w szkicu: struct
`event_ts, event_sequence, event_id`, czyli ta sama krotka co w guard clause notebooka). Ręczne warunki MERGE,
np. inne reguły dla wybranych statusów, byłyby trudniejsze do wyrażenia w deklaratywnym API. Szkic nie był
uruchamiany, więc zgodność zachowania nie jest zweryfikowana. Brakuje też w szkicu `gold_category_sales_dlt`
(analogiczna do daily).

## Jobs API (przykład)

Uruchomienie Joba z parametrami, bez tokena w repozytorium:

```bash
curl --request POST "https://<workspace-host>/api/2.1/jobs/run-now" \
  --header "Authorization: Bearer <TOKEN_FROM_SECRET_MANAGER>" \
  --header "Content-Type: application/json" \
  --data '{
    "job_id": 123456789,
    "job_parameters": {
      "catalog_name": "workspace",
      "schema_name": "de_course_module_03_project",
      "batch_id": "batch_002",
      "run_id": "api_run_001",
      "max_invalid_items": "0"
    }
  }'
```

- **Kiedy:** wywołanie z zewnętrznego orkiestratora (np. Airflow, CI/CD) albo po pojawieniu się nowego pliku batcha.
- **Token:** nie w repozytorium ani w kodzie. W produkcji w Databricks Secret Scope / menedżerze sekretów
  (CI/CD pobiera go w czasie uruchomienia); najlepiej token service principal o minimalnych uprawnieniach.
- **Parametry:** te same, które definiuje `databricks.yml` jako parametry Joba. Użyłem `job_parameters`, bo Job
  ma zdefiniowane parametry; starsze `notebook_params` jest przeznaczone dla jobów bez parametrów Joba.
  Nie testowałem tego requestu — `job_id` jest przykładowy.

## Co Zmieniłbym w Produkcji

1. **Quarantine table zamiast obejścia progu.** Złe rekordy (np. O-111, `qty=-1`) trafiałyby do tabeli kwarantanny
   i nie blokowały Silver, a gate sprawdzałby tylko nowe rekordy z bieżącego batcha. Dziś successful run po
   `batch_003` wymaga podniesienia `max_invalid_items` do 1 — to obejście demo, nie rozwiązanie.
2. **Incremental przetwarzanie Silver Items.** Overwrite całej tabeli skaluje się źle; przy większych danych MERGE po
   kluczu pozycji (np. dodanie `position` z `posexplode`) albo Delta Live Tables / Structured Streaming.
3. **Monitoring i alerty.** Powiadomienia o failed run (e-mail/Slack), metryki jakości zapisywane do tabeli
   (`gold_data_quality_summary`) i dashboard; dziś wyniki bramki są tylko w logu notebooka.
4. **Sekrety i dostęp.** Token do Jobs API w Secret Scope, uruchamianie Joba jako service principal, osobne
   targety `dev`/`prod` w `databricks.yml` (dziś tylko `dev`) i CI/CD z `bundle validate` przed deployem.
5. **Testy i kontrola schematu.** Testy jednostkowe transformacji (Window, `is_valid_item`) na małych danych,
   kontrakt schematu dla źródła (zmiana, np. `source_priority`, wymaga decyzji, a nie cichego pominięcia),
   oraz osobna retencja Time Travel dla tabel, które mają być audytowane (`delta.deletedFileRetentionDuration`).
6. **Test przypadków brzegowych MERGE.** Guard clause jest sprawdzony na danych kursu. Dodałbym test na syntetycznych
   eventach z konfliktem `event_ts` i `event_sequence` oraz z wartościami `NULL`. W Spark SQL `NULL` w polu
   struktury jest traktowany jako wartość mniejsza od każdej innej, więc event z `NULL` w `event_sequence` nigdy
   nie nadpisze wiersza z liczbą, a wiersz z `NULL` zostanie nadpisany — to trzeba świadomie zaakceptować albo
   odrzucić takie eventy w walidacji.
