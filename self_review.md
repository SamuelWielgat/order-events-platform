# Self Review

Wypełnij przed oddaniem projektu.

## Architektura

- [ ] Mam Bronze, Silver i Gold
- [ ] Umiem wyjaśnić grain każdej tabeli
- [ ] Gold liczę z Silver
- [ ] Zachowuję raw eventy w Bronze

## PySpark

- [ ] Używam DataFrame API
- [ ] Używam Window Function do wyboru aktualnego eventu
- [ ] Używam `explode` do rozbicia items
- [ ] Liczę `line_revenue`
- [ ] Nie używam Python UDF tam, gdzie wystarczą funkcje Spark SQL

## Incremental Processing

- [ ] Mam MERGE do aktualizacji Silver
- [ ] Starszy event nie nadpisuje nowszego (guard clause)
- [ ] Duplikaty eventów są obsłużone przed MERGE
- [ ] Ponowne uruchomienie tego samego batcha nie dubluje danych

## Databricks / Delta

- [ ] Używam pełnych nazw tabel (`catalog.schema.table`)
- [ ] Tabele są managed Delta tables
- [ ] Pokazuję `DESCRIBE HISTORY`
- [ ] Pokazuję Time Travel
- [ ] Opisuję schema enforcement i schema evolution

## Spark Internals

- [ ] Mam `explain` dla Window Function
- [ ] Mam `explain` dla joinu z products
- [ ] Mam `explain` dla agregacji Gold
- [ ] Umiem wskazać shuffle
- [ ] Umiem wyjaśnić decyzję o broadcast / cache

## Workflow

- [ ] Mam workflow z minimum 3 taskami
- [ ] Mam parametry Joba
- [ ] Mam quality gate
- [ ] Mam przykład failed run (batch_003, qty=-1)
- [ ] Mam przykład successful run
- [ ] Mam `databricks.yml`

## Komunikacja

- [ ] Mam uzupełnione `engineering_notes.md`
- [ ] Brak tokenów i sekretów w repozytorium
- [ ] Opisuję co zmieniłbym w produkcji
