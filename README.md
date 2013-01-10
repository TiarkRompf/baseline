Baseline
========

This repo contains a few tuned data structure implementations and benchmarks.


Fast and Flexible Hash Indexes
------------------------------

Useful also as part of higher level data structures (Set, Map, MultiMap),
and for custom multi-index relations (like Boost.MultiIndex).


Incremental View Maintenance for Financial Queries
--------------------------------------------------

Layered tree indexes enable O(log n) incremental updates for queries with nested
aggregates and inequality joins. This leads to large speedups for some queries from
the [DBToaster](http://www.dbtoaster.org) benchmark suite.


Build and Run
-------------

        sbt test        // run testsuite
        sbt test:run    // run individual benchmarks


License: AGPLv3