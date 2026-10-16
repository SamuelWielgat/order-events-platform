# Engineering Notes

## Jak Uruchomić Projekt

<!-- TODO: uzupełnić po implementacji -->

- gdzie wgrano dane (Unity Catalog Volume / Workspace Files)
- jakie parametry ustawiać
- kolejność uruchamiania batchy
- jak uruchomić Job / Workflow

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

<!-- TODO: uzupełnić -->

- gdzie używam MERGE
- po jakim kluczu
- jak chronię się przed starszym eventem (guard clause)
- jak obsługuję duplikaty w source

### Broadcast Join

<!-- TODO: uzupełnić -->

- którą tabelę broadcastuję i dlaczego
- gdzie widać to w explain

### Cache

<!-- TODO: uzupełnić -->

- czy używam cache, gdzie i dlaczego (lub dlaczego nie)

## Jakość Danych

<!-- TODO: uzupełnić -->

- jakie walidacje
- co zatrzymuje run
- co jest tylko ostrzeżeniem
- jak obsługuję brakujące produkty (P-404)

## Delta / Unity Catalog

<!-- TODO: uzupełnić po uruchomieniu -->

- pełne nazwy tabel
- przykład DESCRIBE HISTORY
- przykład Time Travel
- przykład schema enforcement (batch_003, source_priority)
- przykład schema evolution

## Spark Explain

<!-- TODO: wkleić fragmenty planów po uruchomieniu -->

### Window Function
### Join z products
### Agregacja Gold

## Co Zmieniłbym w Produkcji

<!-- TODO: uzupełnić -->

1. ...
2. ...
3. ...
