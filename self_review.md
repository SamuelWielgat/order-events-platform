# Self Review

Wypełnij przed oddaniem projektu.

## Architektura

- [x] Mam Bronze, Silver i Gold
- [x] Umiem wyjaśnić grain każdej tabeli
- [x] Gold liczę z Silver
- [x] Zachowuję raw eventy w Bronze

## PySpark

- [x] Używam DataFrame API
- [x] Używam Window Function do wyboru aktualnego eventu
- [x] Używam `explode` do rozbicia items
- [x] Liczę `line_revenue`
- [x] Nie używam Python UDF tam, gdzie wystarczą funkcje Spark SQL

## Incremental Processing

- [x] Mam MERGE do aktualizacji Silver
- [x] Starszy event nie nadpisuje nowszego (guard clause)
- [x] Duplikaty eventów są obsłużone przed MERGE
- [x] Ponowne uruchomienie tego samego batcha nie dubluje danych

## Databricks / Delta

- [x] Używam pełnych nazw tabel (`catalog.schema.table`)
- [x] Tabele są managed Delta tables
- [x] Pokazuję `DESCRIBE HISTORY`
- [x] Pokazuję Time Travel
- [x] Opisuję schema enforcement i schema evolution

## Spark Internals

- [x] Mam `explain` dla Window Function
- [x] Mam `explain` dla joinu z products
- [x] Mam `explain` dla agregacji Gold
- [x] Umiem wskazać shuffle
- [x] Umiem wyjaśnić decyzję o broadcast / cache

## Workflow

- [x] Mam workflow z minimum 3 taskami
- [x] Mam parametry Joba
- [x] Mam quality gate
- [x] Mam przykład failed run (batch_003, qty=-1)
- [x] Mam przykład successful run
- [x] Mam `databricks.yml`

## Komunikacja

- [x] Mam uzupełnione `engineering_notes.md`
- [x] Brak tokenów i sekretów w repozytorium
- [x] Opisuję co zmieniłbym w produkcji
