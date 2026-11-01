# Order Events Platform

Projekt Data Engineering — Databricks pipeline dla eventów zamówień e-commerce
(Bronze → Silver → Gold, MERGE, quality gate, Job + Databricks Asset Bundle).

**Gdzie czytać:** opis decyzji, wyników i ograniczeń jest w [`engineering_notes.md`](engineering_notes.md).

## Mapa Wymagań

| Wymaganie kursu | Gdzie to jest |
|---|---|
| Bronze, managed Delta, jawny schema, kolumny techniczne, idempotencja | `notebooks/01_prepare_bronze.py` |
| Window Function, MERGE z guard clause, explode, broadcast join, flagi jakości, Gold | `notebooks/02_build_silver_and_gold.py` |
| Walidacje jakości i quality gate | `notebooks/03_quality_check.py` |
| `DESCRIBE HISTORY`, Time Travel, schema enforcement i evolution | `notebooks/04_delta_showcases.py` (poza Jobem, ręcznie) |
| Job z 3 taskami, parametry, timeout, retry, `databricks.yml` | `databricks.yml` |
| Alternatywa w DLT (tabela, expectations) | `dlt/dlt_design_example.py` (szkic, niedeployowany) |
| Przykład Jobs API, notatka inżynierska, grain tabel, explain | `engineering_notes.md` |
| Samoocena | `self_review.md` |

## Architektura

```
JSON (batch files)
    │
    ▼
  Bronze            ← raw events, immutable, schema enforced
    │
    ▼
  Silver            ← aktualny stan zamowień + pozycje (MERGE + Window)
    │
    ▼
  Gold              ← agregaty dzienne i kategoriowe (tylko PAID)
    │
    ▼
  Quality Check     ← quality gate, może zatrzymać run
```

## Struktura Repozytorium

```
order-events-platform/
├── notebooks/
│   ├── 01_prepare_bronze.py
│   ├── 02_build_silver_and_gold.py
│   ├── 03_quality_check.py
│   └── 04_delta_showcases.py
├── dlt/
│   └── dlt_design_example.py
├── databricks.yml
├── engineering_notes.md
└── self_review.md
```

## Jak Uruchomić

Pełny opis (dane w Volume, parametry, kolejność batchy, komendy `databricks bundle`) jest w sekcji
„Jak Uruchomić Projekt" w `engineering_notes.md`. W skrócie:

```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run order_events_platform --target dev --params batch_id=batch_001
```

Kolejność batchy: `batch_001` → `batch_002` → `batch_002` (test idempotencji) → `batch_003`.

## Uwaga: `batch_003` zatrzymuje quality gate

`batch_003` zawiera celowo błędny rekord (O-111, `quantity = -1`). Task `quality_check` kończy się wtedy
błędem `Quality gate failed: 1 invalid quantity records found (max_invalid_items=0)` — to zamierzony,
kontrolowany failed run. Rekord zostaje w Silver (jest oznaczony `is_valid_item = false` i nie trafia do Gold),
więc każdy kolejny run z progiem `0` także się zatrzyma. Successful run: ten sam batch z `max_invalid_items=1`
(obejście demonstracyjne; rozwiązanie produkcyjne opisane w `engineering_notes.md`).

## Bezpieczeństwo

W repozytorium nie ma tokenów, sekretów ani prywatnych danych. Databricks CLI korzysta z lokalnego profilu
(`~/.databrickscfg`, poza repozytorium); `.databricks/` i `.databrickscfg` są w `.gitignore`.
Przykład wywołania Jobs API używa placeholdera `<TOKEN_FROM_SECRET_MANAGER>`.