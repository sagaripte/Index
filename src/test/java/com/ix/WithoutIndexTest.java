package com.ix;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The same queries as IndexTest — but written without the ix library.
 *
 * This is how the code typically looks when a developer (or an AI) reaches
 * for the standard Java toolbox: nested Maps, manual loops, streams with
 * repeated scans, and Collectors.groupingBy.
 *
 * Compare each test here with its counterpart in IndexTest to see what
 * ix replaces.
 */
class WithoutIndexTest {

    record Trade(String desk, String region, String status, double pnl, double notional) {}
    record Event(String type, LocalDate date, double amount) {}

    static List<Trade> trades() {
        return List.of(
            new Trade("EQD", "EMEA",  "ACTIVE",   100.0,  1000.0),
            new Trade("EQD", "EMEA",  "ACTIVE",   200.0,  2000.0),
            new Trade("EQD", "APAC",  "CLOSED",   -50.0,   500.0),
            new Trade("FX",  "EMEA",  "ACTIVE",   300.0,  3000.0),
            new Trade("FX",  "APAC",  "ACTIVE",   150.0,  1500.0),
            new Trade("FX",  "APAC",  "CLOSED",  -100.0,   800.0),
            new Trade("IRD", "EMEA",  "ACTIVE",   400.0,  4000.0),
            new Trade("IRD", "APAC",  "CLOSED",   -25.0,   250.0)
        );
    }

    static List<Event> events() {
        return List.of(
            new Event("TRADE", LocalDate.of(2024,  1,  5),  100.0),
            new Event("TRADE", LocalDate.of(2024,  2, 14),  200.0),
            new Event("TRADE", LocalDate.of(2024,  3, 31),  300.0),
            new Event("FEE",   LocalDate.of(2024,  4,  1),   50.0),
            new Event("TRADE", LocalDate.of(2024,  6, 15),  400.0),
            new Event("FEE",   LocalDate.of(2024,  9,  1),   75.0),
            new Event("TRADE", LocalDate.of(2024, 12, 31),  500.0),
            new Event("TRADE", LocalDate.of(2025,  1,  1),  600.0)
        );
    }

    // ── Filter + sum ──────────────────────────────────────────────────────────
    // ix:  Index.of(trades).filter(Trade::status, "ACTIVE").sum(Trade::pnl)

    @Test void sum_active_pnl() {
        double total = 0;
        for (Trade t : trades()) {
            if (t.status().equals("ACTIVE")) total += t.pnl();
        }
        assertEquals(1150.0, total, 0.001);
    }

    // ── Chained filter ────────────────────────────────────────────────────────
    // ix:  idx.filter(Trade::status, "ACTIVE").filter(Trade::region, "EMEA").count()

    @Test void filter_chained() {
        int count = 0;
        for (Trade t : trades()) {
            if (t.status().equals("ACTIVE") && t.region().equals("EMEA")) count++;
        }
        assertEquals(4, count);
    }

    // ── Multi-value OR filter ─────────────────────────────────────────────────
    // ix:  idx.filter(Trade::desk, "EQD", "FX").count()
    // Without ix you either write two conditions or build a Set first.

    @Test void filter_multi_value_or() {
        Set<String> desks = Set.of("EQD", "FX");
        long count = trades().stream()
            .filter(t -> desks.contains(t.desk()))
            .count();
        assertEquals(6, count);
    }

    // ── Repeated filter on the same field ─────────────────────────────────────
    // ix:  root.filter(Trade::region, "EMEA") and root.filter(Trade::region, "APAC")
    //      — index built once on root, reused for both.
    //
    // Without ix: two full scans, no sharing.

    @Test void two_filters_on_same_field() {
        List<Trade> all = trades();

        long emea = all.stream().filter(t -> t.region().equals("EMEA")).count();
        long apac = all.stream().filter(t -> t.region().equals("APAC")).count();

        assertEquals(4, emea);
        assertEquals(4, apac);
        // The list was scanned twice. With ix it would be scanned once (index built on first call).
    }

    // ── GroupBy + sum + count ─────────────────────────────────────────────────
    // ix:  idx.groupBy(Trade::desk).sum(Trade::pnl).count().execute()

    @Test void groupBy_sum_count() {
        // Step 1: group
        Map<String, List<Trade>> byDesk = new LinkedHashMap<>();
        for (Trade t : trades()) {
            byDesk.computeIfAbsent(t.desk(), k -> new ArrayList<>()).add(t);
        }

        // Step 2: aggregate — have to write a loop per metric
        Map<String, Double> pnlByDesk   = new LinkedHashMap<>();
        Map<String, Integer> countByDesk = new LinkedHashMap<>();
        for (Map.Entry<String, List<Trade>> entry : byDesk.entrySet()) {
            double sum = 0;
            for (Trade t : entry.getValue()) sum += t.pnl();
            pnlByDesk.put(entry.getKey(), sum);
            countByDesk.put(entry.getKey(), entry.getValue().size());
        }

        assertEquals(250.0, pnlByDesk.get("EQD"), 0.001);
        assertEquals(3,     countByDesk.get("EQD"));
        assertEquals(350.0, pnlByDesk.get("FX"),  0.001);
        assertEquals(3,     countByDesk.get("FX"));
    }

    // Same test with streams — more concise but still two separate terminal operations
    // (two passes over the data) if you want both sum and count.

    @Test void groupBy_sum_count_streams() {
        Map<String, DoubleSummaryStatistics> stats = trades().stream()
            .collect(Collectors.groupingBy(Trade::desk,
                Collectors.summarizingDouble(Trade::pnl)));

        assertEquals(250.0, stats.get("EQD").getSum(),   0.001);
        assertEquals(3,     stats.get("EQD").getCount());
        assertEquals(350.0, stats.get("FX").getSum(),    0.001);
    }

    // ── GroupBy with pre-filter ───────────────────────────────────────────────
    // ix:  idx.filter(Trade::status, "ACTIVE").groupBy(Trade::desk).sum(Trade::pnl).execute()
    //
    // With streams this is natural. The difference shows up when you run the same
    // filter+groupBy combination multiple times — streams re-scan every time,
    // ix reuses the index.

    @Test void filter_then_groupBy() {
        Map<String, Double> pnlByDesk = trades().stream()
            .filter(t -> t.status().equals("ACTIVE"))
            .collect(Collectors.groupingBy(Trade::desk,
                Collectors.summingDouble(Trade::pnl)));

        assertEquals(300.0, pnlByDesk.get("EQD"), 0.001);  // 100+200
        assertEquals(450.0, pnlByDesk.get("FX"),  0.001);  // 300+150
        assertEquals(400.0, pnlByDesk.get("IRD"), 0.001);
    }

    // ── Multiple groupBy dimensions ───────────────────────────────────────────
    // ix:  idx.filter(f1,v1).filter(f2,v2).groupBy(f3).sum(f4).execute()
    //
    // Without ix you either nest maps or use a composite key string.

    @Test void groupBy_two_dimensions() {
        // Composite key approach — common in enterprise/AI-generated code
        Map<String, Double> pnl = new LinkedHashMap<>();
        for (Trade t : trades()) {
            String key = t.desk() + "|" + t.region();
            pnl.merge(key, t.pnl(), Double::sum);
        }

        assertEquals(300.0,  pnl.get("EQD|EMEA"), 0.001);  // 100+200
        assertEquals(-50.0,  pnl.get("EQD|APAC"), 0.001);
        assertEquals(300.0,  pnl.get("FX|EMEA"),  0.001);
    }

    // ── Distinct ──────────────────────────────────────────────────────────────
    // ix:  idx.distinct(Trade::desk)

    @Test void distinct() {
        // LinkedHashSet to preserve encounter order (same as ix)
        Set<String> seen = new LinkedHashSet<>();
        for (Trade t : trades()) seen.add(t.desk());
        assertEquals(List.of("EQD", "FX", "IRD"), new ArrayList<>(seen));
    }

    // ── Range filter on dates ─────────────────────────────────────────────────
    // ix:  idx.filter(Event::date, between(LocalDate.of(2024,1,1), LocalDate.of(2024,3,31)))
    //
    // Without ix: sort the list first (or scan linearly every time).

    @Test void range_filter_dates_linear_scan() {
        // Linear scan — simple but O(n) every call
        LocalDate lo = LocalDate.of(2024, 1, 1);
        LocalDate hi = LocalDate.of(2024, 3, 31);

        double sum = 0;
        int count = 0;
        for (Event e : events()) {
            if (!e.date().isBefore(lo) && !e.date().isAfter(hi)) {
                sum += e.amount();
                count++;
            }
        }

        assertEquals(3,     count);
        assertEquals(600.0, sum, 0.001);
    }

    @Test void range_filter_dates_presorted() {
        // Pre-sort + binary search — faster per query but you have to manage the sorted copy,
        // and you lose the original order. ix does this automatically and caches the sorted index.
        List<Event> sorted = new ArrayList<>(events());
        sorted.sort(Comparator.comparing(Event::date));

        LocalDate lo = LocalDate.of(2024, 1, 1);
        LocalDate hi = LocalDate.of(2024, 3, 31);

        // Manual binary search for lower bound
        int start = 0;
        while (start < sorted.size() && sorted.get(start).date().isBefore(lo)) start++;
        int end = start;
        while (end < sorted.size() && !sorted.get(end).date().isAfter(hi)) end++;

        double sum = sorted.subList(start, end).stream().mapToDouble(Event::amount).sum();
        assertEquals(600.0, sum, 0.001);
        // The original `events()` list is now out of sync with `sorted`.
        // A second range on a different field requires another sort.
    }

    // ── Two range queries on same field ───────────────────────────────────────
    // ix:  root.filter(Event::date, between(h1_lo, h1_hi))   ← sorted index built once
    //      root.filter(Event::date, between(h2_lo, h2_hi))   ← reused
    //
    // Without ix: sort once and reuse manually — or scan twice.

    @Test void two_range_queries_same_field() {
        // Option A: scan twice — simple, but O(2n)
        LocalDate h1lo = LocalDate.of(2024, 1, 1), h1hi = LocalDate.of(2024, 6, 30);
        LocalDate h2lo = LocalDate.of(2024, 7, 1), h2hi = LocalDate.of(2024, 12, 31);

        long h1 = events().stream()
            .filter(e -> !e.date().isBefore(h1lo) && !e.date().isAfter(h1hi))
            .count();
        long h2 = events().stream()
            .filter(e -> !e.date().isBefore(h2lo) && !e.date().isAfter(h2hi))
            .count();

        assertEquals(5, h1);
        assertEquals(2, h2);
        // Two full scans. With ix: one sort (cached), two binary searches.
    }

    // ── Chained filter then range ─────────────────────────────────────────────
    // ix:  idx.filter(Event::type, "TRADE").filter(Event::date, between(...))

    @Test void chained_equality_then_range() {
        LocalDate lo = LocalDate.of(2024, 1, 1);
        LocalDate hi = LocalDate.of(2024, 3, 31);

        double sum = events().stream()
            .filter(e -> e.type().equals("TRADE"))
            .filter(e -> !e.date().isBefore(lo) && !e.date().isAfter(hi))
            .mapToDouble(Event::amount)
            .sum();

        assertEquals(600.0, sum, 0.001);
        // Fine for a one-off. But if this query runs 1000 times with different date ranges,
        // the equality scan repeats every time. ix builds the type index once and reuses it.
    }

    // ── The "pre-built Map" pattern ───────────────────────────────────────────
    // This is what enterprise code (or an AI without ix) builds when it knows
    // upfront which dimensions it needs. It works — but it requires committing
    // to the structure before you know all the queries.

    @Test void prewired_map_structure() {
        List<Trade> all = trades();

        // Build at startup — one Map per query pattern you anticipate
        Map<String, List<Trade>> byStatus = new LinkedHashMap<>();
        Map<String, List<Trade>> byDesk   = new LinkedHashMap<>();
        Map<String, Map<String, List<Trade>>> byStatusAndDesk = new LinkedHashMap<>();

        for (Trade t : all) {
            byStatus.computeIfAbsent(t.status(), k -> new ArrayList<>()).add(t);
            byDesk.computeIfAbsent(t.desk(), k -> new ArrayList<>()).add(t);
            byStatusAndDesk
                .computeIfAbsent(t.status(), k -> new LinkedHashMap<>())
                .computeIfAbsent(t.desk(), k -> new ArrayList<>())
                .add(t);
        }

        // Now queries are fast — but only for the dimensions you pre-built
        long activeCount = byStatus.getOrDefault("ACTIVE", List.of()).size();
        assertEquals(5, activeCount);

        double eqdPnl = byDesk.getOrDefault("EQD", List.of())
            .stream().mapToDouble(Trade::pnl).sum();
        assertEquals(250.0, eqdPnl, 0.001);

        // What if someone asks for ACTIVE + EQD + EMEA?
        // Not in the pre-built map. You add another level, or scan byStatusAndDesk manually.
        List<Trade> activeEqd = byStatusAndDesk
            .getOrDefault("ACTIVE", Map.of())
            .getOrDefault("EQD", List.of());
        double activeEqdEmea = activeEqd.stream()
            .filter(t -> t.region().equals("EMEA"))
            .mapToDouble(Trade::pnl).sum();
        assertEquals(300.0, activeEqdEmea, 0.001);
        // Third dimension required a manual scan on the already-filtered list.
        // With ix: idx.filter(status,"ACTIVE").filter(desk,"EQD").filter(region,"EMEA").sum(pnl)
    }
}
