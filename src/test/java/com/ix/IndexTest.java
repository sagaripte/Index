package com.ix;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static com.ix.Range.*;
import static org.junit.jupiter.api.Assertions.*;

class IndexTest {

    record Trade(String desk, String region, String status, double pnl, double notional) {}

    // ── Range-filtered Trade (adds a date field) ──────────────────────────────
    record Event(String type, LocalDate date, double amount) {}

    static List<Trade> trades() {
        return List.of(
            new Trade("EQD", "EMEA",  "ACTIVE",   100.0,  1000.0),
            new Trade("EQD", "EMEA",  "ACTIVE",   200.0,  2000.0),
            new Trade("EQD", "APAC",  "CLOSED",  -50.0,   500.0),
            new Trade("FX",  "EMEA",  "ACTIVE",   300.0,  3000.0),
            new Trade("FX",  "APAC",  "ACTIVE",   150.0,  1500.0),
            new Trade("FX",  "APAC",  "CLOSED",  -100.0,  800.0),
            new Trade("IRD", "EMEA",  "ACTIVE",   400.0,  4000.0),
            new Trade("IRD", "APAC",  "CLOSED",   -25.0,  250.0)
        );
    }

    // ── Construction ──────────────────────────────────────────────────────────

    @Test void of_list() {
        Index<Trade> idx = Index.of(trades());
        assertEquals(8, idx.count());
    }

    @Test void of_stream() {
        Index<Trade> idx = Index.of(trades().stream());
        assertEquals(8, idx.count());
    }

    // ── Filter ────────────────────────────────────────────────────────────────

    @Test void filter_single() {
        Index<Trade> active = Index.of(trades()).filter(t -> t.status().equals("ACTIVE"));
        assertEquals(5, active.count());
    }

    @Test void filter_chained() {
        Index<Trade> result = Index.of(trades())
            .filter(t -> t.status().equals("ACTIVE"))
            .filter(t -> t.region().equals("EMEA"));
        assertEquals(4, result.count());
    }

    @Test void filter_returns_typed_objects() {
        List<Trade> result = Index.of(trades())
            .filter(t -> t.desk().equals("FX"))
            .toList();
        assertEquals(3, result.size());
        result.forEach(t -> assertEquals("FX", t.desk()));
    }

    @Test void filter_no_match() {
        Index<Trade> result = Index.of(trades()).filter(t -> t.desk().equals("UNKNOWN"));
        assertEquals(0, result.count());
    }

    // ── Scalar aggregations ───────────────────────────────────────────────────

    @Test void sum() {
        double total = Index.of(trades())
            .filter(t -> t.status().equals("ACTIVE"))
            .sum(Trade::pnl);
        assertEquals(1150.0, total, 0.001);  // 100+200+300+150+400
    }

    @Test void avg() {
        double avg = Index.of(trades())
            .filter(t -> t.status().equals("ACTIVE"))
            .avg(Trade::pnl);
        assertEquals(230.0, avg, 0.001);  // 1150 / 5
    }

    @Test void max() {
        double max = Index.of(trades()).max(Trade::pnl);
        assertEquals(400.0, max, 0.001);
    }

    @Test void min() {
        double min = Index.of(trades()).min(Trade::pnl);
        assertEquals(-100.0, min, 0.001);
    }

    @Test void sum_empty_index() {
        double total = Index.of(trades())
            .filter(t -> t.desk().equals("UNKNOWN"))
            .sum(Trade::pnl);
        assertEquals(0.0, total, 0.001);
    }

    @Test void max_empty_returns_nan() {
        double max = Index.of(trades())
            .filter(t -> t.desk().equals("UNKNOWN"))
            .max(Trade::pnl);
        assertTrue(Double.isNaN(max));
    }

    // ── Distinct ─────────────────────────────────────────────────────────────

    @Test void distinct() {
        List<String> desks = Index.of(trades()).distinct(Trade::desk);
        assertEquals(List.of("EQD", "FX", "IRD"), desks);
    }

    @Test void distinct_after_filter() {
        List<String> desks = Index.of(trades())
            .filter(t -> t.region().equals("APAC"))
            .distinct(Trade::desk);
        assertEquals(List.of("EQD", "FX", "IRD"), desks);
    }

    @Test void distinct_high_cardinality_does_not_throw() {
        // distinct() must not call getOrBuildIndex() — high-cardinality fields would throw
        record Tagged(String id, String desk) {}
        List<Tagged> data = List.of(
            new Tagged("T1", "EQD"), new Tagged("T2", "FX"),
            new Tagged("T3", "IRD"), new Tagged("T4", "EQD"),
            new Tagged("T5", "FX"),  new Tagged("T6", "IRD")
        );
        Index<Tagged> idx = Index.of(data);
        // 6 distinct ids across 6 rows = 100% cardinality — filter() would throw, distinct() must not
        List<String> ids = assertDoesNotThrow(() -> idx.distinct(Tagged::id));
        assertEquals(6, ids.size());
    }

    @Test void distinct_uses_existing_index_when_available() {
        // After a filter() call builds the index, distinct() should use it
        Index<Trade> idx = Index.of(trades());
        idx.filter(Trade::desk, "EQD");  // builds desk index
        List<String> desks = idx.distinct(Trade::desk);
        assertEquals(3, desks.size());
        assertTrue(desks.contains("EQD"));
        assertTrue(desks.contains("FX"));
        assertTrue(desks.contains("IRD"));
    }

    // ── GroupBy ───────────────────────────────────────────────────────────────

    @Test void groupBy_sum_count() {
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::desk)
            .sum(Trade::pnl)
            .count()
            .execute();

        assertEquals(3, result.groupCount());

        Aggregation.Row<String> eqd = result.get("EQD");
        assertNotNull(eqd);
        assertEquals(250.0, eqd.sum(0), 0.001);   // 100+200-50
        assertEquals(3,     eqd.count());

        Aggregation.Row<String> fx = result.get("FX");
        assertNotNull(fx);
        assertEquals(350.0, fx.sum(0), 0.001);    // 300+150-100
        assertEquals(3,     fx.count());
    }

    @Test void groupBy_avg() {
        Aggregation.Results<String> result = Index.of(trades())
            .filter(t -> t.status().equals("ACTIVE"))
            .groupBy(Trade::desk)
            .avg(Trade::pnl)
            .execute();

        Aggregation.Row<String> eqd = result.get("EQD");
        assertNotNull(eqd);
        assertEquals(150.0, eqd.avg(0), 0.001);   // (100+200)/2
    }

    @Test void groupBy_min_max() {
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::region)
            .min(Trade::pnl)
            .max(Trade::pnl)
            .execute();

        Aggregation.Row<String> emea = result.get("EMEA");
        assertEquals(100.0, emea.min(0), 0.001);
        assertEquals(400.0, emea.max(0), 0.001);

        Aggregation.Row<String> apac = result.get("APAC");
        assertEquals(-100.0, apac.min(0), 0.001);
        assertEquals(150.0,  apac.max(0), 0.001);
    }

    @Test void groupBy_multiple_sums() {
        Aggregation.Results<String> result = Index.of(trades())
            .filter(t -> t.status().equals("ACTIVE"))
            .groupBy(Trade::desk)
            .sum("pnl",      Trade::pnl)
            .sum("notional", Trade::notional)
            .count()
            .execute();

        Aggregation.Row<String> ird = result.get("IRD");
        assertNotNull(ird);
        assertEquals(400.0,  ird.sum("pnl"),      0.001);
        assertEquals(4000.0, ird.sum("notional"),  0.001);
        assertEquals(1,      ird.count());
    }

    @Test void groupBy_named_access() {
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::desk)
            .sum("pnl",      Trade::pnl)
            .avg("avgPnl",   Trade::pnl)
            .min("minPnl",   Trade::pnl)
            .max("maxPnl",   Trade::pnl)
            .execute();

        Aggregation.Row<String> eqd = result.get("EQD");
        assertEquals(250.0,             eqd.sum("pnl"),    0.001);  // 100+200-50
        assertEquals(250.0 / 3,         eqd.avg("avgPnl"), 0.001);
        assertEquals(-50.0,             eqd.min("minPnl"), 0.001);
        assertEquals(200.0,             eqd.max("maxPnl"), 0.001);
    }

    @Test void groupBy_named_unknown_throws() {
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::desk)
            .sum("pnl", Trade::pnl)
            .execute();

        Aggregation.Row<String> eqd = result.get("EQD");
        assertThrows(IllegalArgumentException.class, () -> eqd.sum("notional"));
    }

    @Test void groupBy_key_is_typed() {
        // key() returns K — String here, no cast needed
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::desk)
            .count()
            .execute();

        for (Aggregation.Row<String> row : result) {
            String desk = row.key();   // compile-time typed
            assertNotNull(desk);
        }
    }

    // ── High-cardinality guard ────────────────────────────────────────────────

    @Test void high_cardinality_field_throws() {
        // Each trade has a unique synthetic id — 8 distinct values across 8 rows = 100%
        record TaggedTrade(String id, String desk) {}
        List<TaggedTrade> data = List.of(
            new TaggedTrade("t1", "EQD"), new TaggedTrade("t2", "EQD"),
            new TaggedTrade("t3", "FX"),  new TaggedTrade("t4", "FX"),
            new TaggedTrade("t5", "IRD"), new TaggedTrade("t6", "IRD"),
            new TaggedTrade("t7", "EQD"), new TaggedTrade("t8", "FX")
        );
        Index<TaggedTrade> idx = Index.of(data);
        // "desk" has 3 distinct values across 8 rows (37%) — fine
        assertDoesNotThrow(() -> idx.filter(TaggedTrade::desk, "EQD"));
        // "id" has 8 distinct values across 8 rows (100%) — should throw
        assertThrows(IllegalArgumentException.class,
            () -> idx.filter(TaggedTrade::id, "t1"));
    }

    // ── Indexed filter (field equality) ──────────────────────────────────────

    @Test void indexed_filter_single_value() {
        Index<Trade> idx = Index.of(trades());
        assertEquals(4, idx.filter(Trade::region, "EMEA").count());
        assertEquals(4, idx.filter(Trade::region, "APAC").count());
    }

    @Test void indexed_filter_multi_value_or() {
        // "EQD" OR "FX" = 6 trades
        Index<Trade> result = Index.of(trades()).filter(Trade::desk, "EQD", "FX");
        assertEquals(6, result.count());
    }

    @Test void indexed_filter_no_match() {
        assertEquals(0, Index.of(trades()).filter(Trade::desk, "UNKNOWN").count());
    }

    @Test void indexed_filter_chained_with_indexed() {
        // USD desk "EQD" EMEA ACTIVE → trades 0,1 only (pnl 100, 200)
        Index<Trade> result = Index.of(trades())
            .filter(Trade::desk, "EQD")
            .filter(Trade::region, "EMEA")
            .filter(Trade::status, "ACTIVE");
        assertEquals(2, result.count());
        assertEquals(300.0, result.sum(Trade::pnl), 0.001);
    }

    @Test void indexed_filter_chained_with_predicate() {
        // index lookup then predicate scan on reduced set
        Index<Trade> result = Index.of(trades())
            .filter(Trade::status, "ACTIVE")
            .filter(t -> t.pnl() > 200);
        assertEquals(2, result.count());   // 300.0 (FX/EMEA) and 400.0 (IRD/EMEA)
    }

    @Test void index_is_shared_across_derived_views() {
        // Both filtered views should share the same index on the root
        Index<Trade> root = Index.of(trades());
        Index<Trade> emea = root.filter(Trade::region, "EMEA");
        Index<Trade> apac = root.filter(Trade::region, "APAC");
        // If index wasn't shared, second call would rebuild — we can't observe that directly,
        // but correctness of both views proves shared state works
        assertEquals(4, emea.count());
        assertEquals(4, apac.count());
    }

    @Test void indexed_filter_results_are_correct_objects() {
        List<Trade> result = Index.of(trades())
            .filter(Trade::desk, "IRD")
            .toList();
        assertEquals(2, result.size());
        result.forEach(t -> assertEquals("IRD", t.desk()));
    }

    @Test void indexed_groupby_sum() {
        Aggregation.Results<String> result = Index.of(trades())
            .filter(Trade::status, "ACTIVE")
            .groupBy(Trade::desk)
            .sum(Trade::pnl)
            .count()
            .execute();

        assertEquals(3, result.groupCount());
        assertEquals(300.0, result.get("EQD").sum(0), 0.001);  // 100+200
        assertEquals(450.0, result.get("FX").sum(0),  0.001);  // 300+150
        assertEquals(400.0, result.get("IRD").sum(0), 0.001);
    }

    // ── Stream / iteration ────────────────────────────────────────────────────

    @Test void stream() {
        long count = Index.of(trades())
            .filter(t -> t.region().equals("EMEA"))
            .stream()
            .filter(t -> t.pnl() > 150)
            .count();
        assertEquals(3, count);   // EMEA trades: 100, 200, 300, 400 → > 150: 200, 300, 400
    }

    @Test void iterator_over_filtered() {
        int seen = 0;
        for (Trade t : Index.of(trades()).filter(t -> t.desk().equals("IRD"))) {
            assertEquals("IRD", t.desk());
            seen++;
        }
        assertEquals(2, seen);
    }

    // ── Range filter ──────────────────────────────────────────────────────────

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

    @Test void range_between_dates() {
        // Q1 2024: Jan 1 – Mar 31
        Index<Event> q1 = Index.of(events()).filter(
            Event::date,
            between(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 31))
        );
        assertEquals(3, q1.count());
        assertEquals(600.0, q1.sum(Event::amount), 0.001);  // 100+200+300
    }

    @Test void range_after_date() {
        Index<Event> result = Index.of(events()).filter(
            Event::date, after(LocalDate.of(2024, 6, 15))
        );
        assertEquals(4, result.count());  // Jun 15 (inclusive), Sep 1, Dec 31, Jan 1 2025
    }

    @Test void range_before_date() {
        Index<Event> result = Index.of(events()).filter(
            Event::date, before(LocalDate.of(2024, 4, 1))
        );
        assertEquals(4, result.count());  // Jan 5, Feb 14, Mar 31, Apr 1 (inclusive)
    }

    @Test void range_above_exclusive() {
        // above(200) = strictly > 200
        Index<Event> result = Index.of(events()).filter(Event::amount, above(200.0));
        assertEquals(4, result.count());  // 300, 400, 500, 600 — not 200
    }

    @Test void range_below_exclusive() {
        // below(200) = strictly < 200
        Index<Event> result = Index.of(events()).filter(Event::amount, below(200.0));
        assertEquals(3, result.count());  // 100, 50, 75 — not 200
    }

    @Test void range_no_match() {
        Index<Event> result = Index.of(events()).filter(
            Event::date, between(LocalDate.of(2030, 1, 1), LocalDate.of(2030, 12, 31))
        );
        assertEquals(0, result.count());
    }

    @Test void range_chained_with_equality_filter() {
        // Q1 2024 TRADE events only
        Index<Event> result = Index.of(events())
            .filter(Event::type,  "TRADE")
            .filter(Event::date,  between(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 31)));
        assertEquals(3, result.count());
        assertEquals(600.0, result.sum(Event::amount), 0.001);
    }

    @Test void range_numeric_between() {
        Index<Event> result = Index.of(events()).filter(
            Event::amount, between(100.0, 400.0)
        );
        assertEquals(4, result.count());  // 100, 200, 300, 400 — 50 and 75 excluded, 500/600 excluded
    }

    @Test void range_sorted_index_reused() {
        // Two range queries on same field — sorted index built once
        Index<Event> root = Index.of(events());
        Index<Event> h1 = root.filter(Event::date,
            between(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 6, 30)));
        Index<Event> h2 = root.filter(Event::date,
            between(LocalDate.of(2024, 7, 1), LocalDate.of(2024, 12, 31)));
        assertEquals(5, h1.count());  // Jan5, Feb14, Mar31, Apr1, Jun15
        assertEquals(2, h2.count());  // Sep1, Dec31
        assertEquals(7, h1.count() + h2.count());
    }

    @Test void range_groupby_after_range_filter() {
        Aggregation.Results<String> result = Index.of(events())
            .filter(Event::date, after(LocalDate.of(2024, 1, 1)))
            .filter(Event::type, "TRADE")
            .groupBy(Event::type)
            .sum(Event::amount)
            .count()
            .execute();

        assertEquals(1, result.groupCount());
        Aggregation.Row<String> row = result.get("TRADE");
        assertNotNull(row);
        // TRADE events after(inclusive) Jan 1: Jan5(100), Feb14(200), Mar31(300), Jun15(400), Dec31(500), Jan1 2025(600)
        assertEquals(6, row.count());
        assertEquals(2100.0, row.sum(0), 0.001);
    }

    // ── Regression: Double range filter with negative values ──────────────────
    // Exercises the LongCodec.DOUBLE sign-bit flip fix (#4).

    @Test void range_double_negative_values() {
        record Pos(double value) {}
        List<Pos> data = List.of(
            new Pos(-100.0), new Pos(-50.0), new Pos(0.0),
            new Pos(50.0),   new Pos(100.0)
        );
        Index<Pos> idx = Index.of(data);

        // above(-50) = strictly > -50: 0, 50, 100
        assertEquals(3, idx.filter(Pos::value, above(-50.0)).count());

        // below(50) = strictly < 50: -100, -50, 0
        assertEquals(3, idx.filter(Pos::value, below(50.0)).count());

        // between(-50, 50) inclusive: -50, 0, 50
        assertEquals(3, idx.filter(Pos::value, between(-50.0, 50.0)).count());

        // sum of negatives only (below 0 exclusive)
        double negSum = idx.filter(Pos::value, below(0.0)).sum(Pos::value);
        assertEquals(-150.0, negSum, 0.001);
    }

    // ── Regression: LocalDateTime codec millisecond precision ─────────────────
    // Exercises the LongCodec.LOCAL_DATE_TIME millisecond fix (#8).

    @Test void range_localdatetime_millisecond_precision() {
        record Ts(LocalDateTime ts) {}
        LocalDateTime base = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        List<Ts> data = List.of(
            new Ts(base),                                        // exactly at boundary
            new Ts(base.plusNanos(500_000_000)),                 // +500ms
            new Ts(base.plusNanos(999_000_000)),                 // +999ms
            new Ts(base.plusSeconds(1))                          // +1s (outside above boundary)
        );
        Index<Ts> idx = Index.of(data);

        // above(base) = strictly after base: +500ms, +999ms, +1s
        assertEquals(3, idx.filter(Ts::ts, above(base)).count());

        // between base and base+999ms (inclusive): base, +500ms, +999ms
        assertEquals(3, idx.filter(Ts::ts,
            between(base, base.plusNanos(999_000_000))).count());
    }

    // ── Unique lookup ─────────────────────────────────────────────────────────

    @Test void unique_found() {
        record Tagged(String id, String desk) {}
        List<Tagged> data = List.of(
            new Tagged("T1", "EQD"),
            new Tagged("T2", "FX"),
            new Tagged("T3", "IRD")
        );
        Index<Tagged> idx = Index.of(data);
        Tagged t = idx.unique(Tagged::id, "T2");
        assertNotNull(t);
        assertEquals("FX", t.desk());
    }

    @Test void unique_not_found_returns_null() {
        record Tagged(String id, String desk) {}
        List<Tagged> data = List.of(new Tagged("T1", "EQD"), new Tagged("T2", "FX"));
        assertNull(Index.of(data).unique(Tagged::id, "T99"));
    }

    @Test void unique_on_filtered_view_respects_active_rows() {
        record Tagged(String id, String desk) {}
        List<Tagged> data = List.of(
            new Tagged("T1", "EQD"),
            new Tagged("T2", "FX"),
            new Tagged("T3", "EQD"),
            new Tagged("T4", "EQD"),
            new Tagged("T5", "FX"),
            new Tagged("T6", "EQD")
        );
        Index<Tagged> eqd = Index.of(data).filter(Tagged::desk, "EQD");
        // T1, T3, T4, T6 are active; T2 and T5 (FX) are not
        assertNotNull(eqd.unique(Tagged::id, "T1"));
        assertNull(eqd.unique(Tagged::id, "T2"));
        assertNotNull(eqd.unique(Tagged::id, "T3"));
        assertNull(eqd.unique(Tagged::id, "T5"));
    }

    @Test void unique_does_not_throw_on_high_cardinality_field() {
        // unique() bypasses the cardinality guard that filter() enforces
        record Tagged(String id, String desk) {}
        List<Tagged> data = List.of(
            new Tagged("T1", "EQD"), new Tagged("T2", "FX"),
            new Tagged("T3", "IRD"), new Tagged("T4", "EQD")
        );
        // 4 distinct ids across 4 rows = 100% cardinality — fine for unique()
        assertDoesNotThrow(() -> Index.of(data).unique(Tagged::id, "T1"));
    }

    // ── Regression: Aggregation count precision (stored as long, not double) ──
    // Exercises fix #2.

    @Test void groupby_count_is_exact_long() {
        Aggregation.Results<String> result = Index.of(trades())
            .groupBy(Trade::desk)
            .count()
            .execute();
        // Verify count() returns a proper long, not a truncated double
        assertEquals(3L, result.get("EQD").count());
        assertEquals(3L, result.get("FX").count());
        assertEquals(2L, result.get("IRD").count());
    }
}
