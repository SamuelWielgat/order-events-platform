# Order Events Platform

Projekt Data Engineering — Databricks pipeline dla eventów zamówień e-commerce.

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
│   └── 03_quality_check.py
├── dlt/
│   └── dlt_design_example.py
├── databricks.yml
├── implementation_plan.md
├── engineering_notes.md
└── self_review.md
```

## Jak Uruchomić

Szczegóły w `engineering_notes.md`.

## Bezpieczeństwo

Brak tokenów, sekretów i prywatnych danych w repozytorium.
Wszystkie credentials przez Databricks Secret Scope.