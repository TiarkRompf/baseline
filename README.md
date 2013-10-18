Baseline
========

This repo contains a few tuned data structure implementations and benchmarks.


Stream API for Collections
--------------------------

High-level collection operations without intermediate data structures:

    def evenOdd(x: Int) = if (x % 2 == 0) "even" else "odd"

    val reducer = Stream[Int] filter (_ > 0) groupBy (evenOdd, _.sum)

    val result = List(-1,-2,-3,1,2,3,4,5,6) into reducer

    assert(result === Map("odd" -> 9, "even" -> 12))

Just one Map[String,Int] is created to hold the final results.



Fast and Flexible Hash Indexes
------------------------------

Useful also as part of higher level data structures (Set, Map, MultiMap),
and for custom multi-index relations (like Boost.MultiIndex).

Here is an example of implementing a HashSet using
HashIndexImpl:

    class HashSet[A:ClassManifest] {
      val data = new BufferImpl[A]
      val index = new HashIndexImpl(data(_))
      
      def +=(x: A): this.type = {
        if (index.focus(x) == -1) {
          data += x
          index.putValue(data.size-1)
        }
        this
      }
      
      def contains(x: A): Boolean = index.get(x) != -1  
    }


Aggregate Tree Indexes
----------------------

Based on (left-leaning) Red-Black trees. Aggregates are maintained
for values *and* keys. This enables O(log n) collective operations not
only on values (e.g. sum, min, max) but also on key sets 
(e.g. shifts of index ranges).


Incremental View Maintenance for Financial Queries
--------------------------------------------------

Layered tree indexes enable O(log n) incremental updates for queries with nested
aggregates and inequality joins. This leads to large speedups for some queries from
the [DBToaster](http://www.dbtoaster.org) benchmark suite.

Even complex queries like this can be computed fully incrementally:

      val resm = new MutableTreeImpl

      bids.foreach { b =>
        asks.foreach { a =>

          val c1 = 0.25 * asks.map(_.volume).sum > asks.filter(_.price > a.price).map(_.volume).sum
          val c2 = 0.25 * bids.map(_.volume).sum > bids.filter(_.price > b.price).map(_.volume).sum

          if (c1 && c2)
            resm.add(b.broker_id, a.price * a.volume * b.X - b.price * b.volume * a.X)  // need to include both a.X and b.X
        }
      }




Build and Run
-------------

        sbt test        // run testsuite
        sbt test:run    // run individual benchmarks


License: AGPLv3