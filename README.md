# IX

**IX turns a list of POJOs into a queryable index — filter and aggregate without building maps.**

The usual Java approach to in-memory lookups is building `Map` objects — one per query shape, nested deeper as you add dimensions:

```java
// Standard Java — structure committed before you know all your queries
Map<String, Map<String, List<Trade>>> byStatusAndDesk = new LinkedHashMap<>();
for (Trade t : trades) {
    byStatusAndDesk
        .computeIfAbsent(t.getStatus(), k -> new LinkedHashMap<>())
        .computeIfAbsent(t.getDesk(), k -> new ArrayList<>())
        .add(t);
}
// Need a third dimension? Add another level of nesting.
// Need sum instead of list? Rewrite the whole thing.
```

IX wraps a collection and provides fluent filter and aggregate methods:

```java
Index<Trade> idx = Index.of(trades);

Index<Trade> slice = idx
    .filter(Trade::getDesk, "EQD")
    .filter(Trade::getRegion, "EMEA")
    .filter(Trade::getStatus, "ACTIVE");

double pnl_sum = slice.sum(Trade::getPnl);

for (Trade t : slice) {
    // business logic
}
```

Add a filter on any field without restructuring anything — the index for that field is built on first use and shared across every query derived from the same root.

The sweet spot is **read-heavy, snapshot-style data in the 10K–500K row range** — an end-of-day job, a reporting engine rendering a summary table with drilldowns, anything that loads a snapshot and slices it multiple ways.

It doesn't fit everywhere. One fixed lookup? Use a `HashMap`. Data changing row by row? IX is rebuild-only. Need joins? Use a database. A few million rows? Reach for DuckDB or Arrow instead.

---

## API

```java
import com.ix.Index;
import static com.ix.Range.*;

// Construction — O(1), no index built yet
Index<Trade> idx = Index.of(trades);          // List<T>
Index<Trade> idx = Index.of(trades.stream()); // Stream<T>

// Equality filter — O(1) after first call per field
// First call scans once and builds an inverted index; subsequent calls hit the cache.
idx.filter(Trade::getStatus, "ACTIVE")
idx.filter(Trade::getDesk, "EQD", "FX")   // multi-value OR

// Predicate filter — always O(n) scan
idx.filter(t -> t.getPnl() > 1_000_000)

// Range filter — O(log n + k) after first call per field
idx.filter(Event::getDate, between(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 31)))
idx.filter(Event::getDate, after(LocalDate.of(2024, 6, 1)))
idx.filter(Event::getAmount, below(500.0))

// All filter types chain
Index<Trade> slice = idx
    .filter(Trade::getDesk, "EQD")
    .filter(Trade::getRegion, "EMEA")
    .filter(t -> t.getStatus().equals("ACTIVE"));

// Scalar aggregations
double total = slice.sum(Trade::getPnl);
double avg   = slice.avg(Trade::getPnl);
double max   = slice.max(Trade::getPnl);
double min   = slice.min(Trade::getPnl);
int    count = slice.count();

// GroupBy
Aggregation.Results<String> byDesk = idx
    .filter(Trade::getStatus, "ACTIVE")
    .groupBy(Trade::getDesk)
    .sum("pnl",      Trade::getPnl)
    .sum("notional", Trade::getNotional)
    .count()
    .execute();

for (Aggregation.Row<String> row : byDesk) {
    System.out.println(row.key() + "  pnl=" + row.sum("pnl") + "  notional=" + row.sum("notional"));
}

// Unique lookup — for high-cardinality fields (trade ID, UUID, etc.)
// Builds a HashMap on first call, O(1) after. Does not throw on high cardinality.
Trade t = idx.unique(Trade::getId, "T-001");  // null if not found

// Distinct values
List<String> desks = idx.distinct(Trade::getDesk);  // ["EQD", "FX", "IRD"]

// Back to your domain type
List<Trade>   list   = slice.toList();
Stream<Trade> stream = slice.stream();
for (Trade t : slice) { ... }  // Iterable<T>
```

`Range` factories: `between(lo, hi)`, `after(lo)`, `before(hi)` are inclusive. `above(lo)`, `below(hi)` are exclusive.

---

## How the indexes work

**Equality index.** On the first `filter(field, value)` call for a field, IX scans the full dataset once and builds one bit vector per distinct value. `filter(desk, "EQD")` sets a bit for every EQD row. Every subsequent call — any value, any derived filter chain — does a bitmap lookup and an AND with the current active set. No scan.

The index lives on the root `Index<T>` and is shared across all derived views. Build it once, use it everywhere.

**Sorted index.** On the first `filter(field, range)` call for a field, IX sorts two parallel arrays — encoded values and original row positions. Range queries binary-search for the bounds and mark the matching rows. Numeric types and dates are stored as `long` primitives (8 bytes/row); strings fall back to `Comparable<?>[]`.

**Memory.** For 100K rows, 5 indexed fields:

| | Per row per field | 100K × 5 fields |
|---|---|---|
| Equality bitmaps | ~1.6 bytes | ~800 KB |
| Sorted index | 12 bytes | ~6 MB |
| Your objects | unchanged | unchanged |

**High-cardinality guard.** If more than 50% of rows have a unique value for a field (e.g. a trade ID), IX throws rather than building a bitmap that would be mostly empty. Use `filter(Predicate)` or `unique()` for those fields.

---

## Limitations

- No joins. Single dataset only.
- Read-only after construction. Adding rows means rebuilding: `Index.of(updatedList)`.
- Sweet spot is 10K–500K rows. Works up to a few million; above that you want a columnar store.
- `groupBy` takes one key. For composite keys, build a key object or string yourself.

---

## Building

```bash
./gradlew test
```

Java 17+. No dependencies.
