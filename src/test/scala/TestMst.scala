package baseline


import baseline.collection._

import java.io.PrintWriter
import scala.collection._

/*
CREATE STREAM bids(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE 'examples/data/finance.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

CREATE STREAM asks(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE 'examples/data/finance.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10', 
                            deterministic := 'yes');

SELECT b.broker_id, sum((a.price * a.volume) + (-1 * b.price * b.volume)) AS mst
FROM bids b, asks a
WHERE 0.25*(SELECT sum(a1.volume) FROM asks a1) >
           (SELECT sum(a2.volume) FROM asks a2 WHERE a2.price > a.price)
AND   0.25*(SELECT sum(b1.volume) FROM bids b1) >
           (SELECT sum(b2.volume) FROM bids b2 WHERE b2.price > b.price)
GROUP BY b.broker_id;
*/




class TestMst extends FileDiffSuite {

  val prefix = "test-out/test12-"

  def test1 = withOutFileChecked(prefix + "mst1") {

    // starting point

    val o = new BulkReader; import o._
    o.debug = true
    o.run {
      val resm = new MutableTreeImpl

      bids.foreach { b =>
        asks.foreach { a =>

          val c1 = 0.25 * asks.map(_.volume).sum > asks.filter(_.price > a.price).map(_.volume).sum
          val c2 = 0.25 * bids.map(_.volume).sum > bids.filter(_.price > b.price).map(_.volume).sum

          if (c1 && c2)
            resm.add(b.broker_id, a.price * a.volume * b.X - b.price * b.volume * a.X)  // need to include both a.X and b.X
        }
      }

      println("mst: " + resm)
    }
  }


  def test2 = withOutFileChecked(prefix + "mst2") {

    // factoring independent stuff

    val o = new BulkReader; import o._
    o.debug = true
    o.run {

      val resm = new MutableTreeImpl

      bids.foreach { b =>

        val c2 = 0.25 * bids.map(_.volume).sum > bids.filter(_.price > b.price).map(_.volume).sum

        if (c2) {

          val bVal = b.price * b.volume

          var aCount = 0
          var aSum = 0.0

          asks.foreach { a =>

            val c1 = 0.25 * asks.map(_.volume).sum > asks.filter(_.price > a.price).map(_.volume).sum

            if (c1) {
              val aVal = a.price * a.volume

              aCount += a.X // <--- subtract if removing
              aSum += aVal
            }

          }

          resm.add(b.broker_id, b.X * aSum - aCount * bVal) // <--- all negative if removing
        }

      }

      println("mst: " + resm)
    }
  }


  def test3 = withOutFileChecked(prefix + "mst3") {

    // moving loops

    val o = new BulkReader; import o._
    o.debug = true
    o.run {

      val aSum3 = asks.map(_.volume).sum
      def aSum2(a_price: Double) = asks.filter(_.price <= a_price).map(_.volume).sum

      var aResCount = 0
      var aResSum = 0.0

      asks.foreach { a =>

        val c1 = 0.75 * aSum3 < aSum2(a.price)

        if (c1) {
          val aVal = a.price * a.volume

          aResCount += a.X // <--- subtract if removing
          aResSum += aVal
        }
      }


      val bSum3 = bids.map(_.volume).sum
      def bSum2(b_price: Double) = bids.filter(_.price <= b_price).map(_.volume).sum

      val resm = new MutableTreeImpl

      bids.foreach { b =>

        val c2 = 0.75 * bSum3 < bSum2(b.price)

        if (c2) {
          val bVal = b.price * b.volume

          resm.add(b.broker_id, b.X * aResSum - aResCount * bVal)  // <--- all negative if removing
        }
      }

      println("mst: " + resm)
    }
  }




  def test4 = withOutFileChecked(prefix + "mst4") {

    // tree maps

    val o = new BulkReader; import o._
    o.debug = true
    o.run {

      val aMap = new MutableTreeImpl
      asks.foreach { a => aMap.add(a.price, a.volume) }

      val aSum3 = aMap.getSum
      def aSum2(a_price: Double) = aMap.getSum(a_price)

      var aResCount = 0
      var aResSum = 0.0

      asks.foreach { a =>

        val c1 = 0.75 * aSum3 < aSum2(a.price)

        if (c1) {
          val aVal = a.price * a.volume

          aResCount += a.X // <--- subtract if removing
          aResSum += aVal
        }
      }



      val bMap = new MutableTreeImpl
      bids.foreach { b => bMap.add(b.price, b.volume) }

      val bSum3 = bMap.getSum
      def bSum2(b_price: Double) = bMap.getSum(b_price)

      val resm = new MutableTreeImpl

      bids.foreach { b =>

        val c2 = 0.75 * bSum3 < bSum2(b.price)

        if (c2) {
          val bVal = b.price * b.volume

          resm.add(b.broker_id, b.X * aResSum - aResCount * bVal)  // <--- all negative if removing
        }
      }

      println("mst: " + resm)
    }

  }



  def test5 = withOutFileChecked(prefix + "mst5") {

    // secondary maps

    val o = new BulkReader; import o._
    o.debug = true    
    o.run {

      val aMap = new MutableTreeImpl
      asks.foreach { a => aMap.add(a.price, a.volume) } // REMOVE

      val aResCountMap = new MutableTreeImpl
      val aResSumMap = new MutableTreeImpl

      asks.foreach { a =>
        val aSum2 = aMap.getSum(a.price) // INDEX THIS
        aResCountMap.add(aSum2, a.X)
        aResSumMap.add(aSum2, a.price * a.volume)
      }

      val aResCount = aResCountMap.getSum - aResCountMap.getSum(0.75 * aMap.getSum)
      val aResSum = aResSumMap.getSum - aResSumMap.getSum(0.75 * aMap.getSum)

      // ---

      val bResBrokerMap = new Array[MutableTreeImpl](10) // result per broker
      for (i <- 0 until 10) bResBrokerMap(i) = new MutableTreeImpl

      val bMap = new MutableTreeImpl
      bids.foreach { b => bMap.add(b.price, b.volume) } // REMOVE

      bids.foreach { b =>
        val bSum2 = bMap.getSum(b.price) // INDEX THIS
        val bVal = b.price * b.volume

        val resMap = bResBrokerMap(b.broker_id.toInt)
        resMap.add(bSum2, b.X * aResSum - aResCount * bVal)  // <--- all negative if removing
      }

      val res = (0 until 10).toList.map { i =>
        val sum = bResBrokerMap(i).getSum - bResBrokerMap(i).getSum(0.75 * bMap.getSum)
        if (sum != 0) i.toDouble + ":" + sum + " "
        else ""
      }

      println("mst: " + res.mkString)
    }

  }


  def test6 = withOutFileChecked(prefix + "mst6") {

    // incrementalize individual maps

    val o = new BulkReader; import o._
    o.debug = true
    o.run {

      val aMap = new MutableTreeImpl

      val aResCountMap = new MutableTreeImpl
      val aResSumMap = new MutableTreeImpl

      asks.foreach { a =>
        val aSum2 = aMap.getSum(a.price)
        val aVal3 = aMap.get(a.price)

        aResSumMap.shiftKeys(aSum2-aVal3, a.volume) // add volume to existing sums
        aResSumMap.add(aSum2 + a.volume, a.price * a.volume) // add at new position

        aResCountMap.shiftKeys(aSum2-aVal3, a.volume) // add volume to existing sums
        aResCountMap.add(aSum2 + a.volume, a.X) // add at new position

        aMap.add(a.price, a.volume)
      }

      val aResCount = aResCountMap.getSum - aResCountMap.getSum(0.75 * aMap.getSum)
      val aResSum = aResSumMap.getSum - aResSumMap.getSum(0.75 * aMap.getSum)

      // ---

      val bResBrokerMap = new Array[MutableTreeImpl](10) // result per broker
      for (i <- 0 until 10) bResBrokerMap(i) = new MutableTreeImpl

      val bMap = new MutableTreeImpl

      bids.foreach { b =>
        val bSum2 = bMap.getSum(b.price)
        val bVal3 = bMap.get(b.price)

        // update all broker maps
        for (i <- 0 until 10) bResBrokerMap(i).shiftKeys(bSum2-bVal3, b.volume)

        val bVal = b.price * b.volume
        val resMap = bResBrokerMap(b.broker_id.toInt)
        resMap.add(bSum2 + b.volume, b.X * aResSum - aResCount * bVal)  // depends on aResSum, aResCount

        bMap.add(b.price, b.volume)
      }

      val res = (0 until 10).toList.map { i =>

        val sum = bResBrokerMap(i).getSum - bResBrokerMap(i).getSum(0.75 * bMap.getSum)
        
        if (sum != 0) i.toDouble + ":" + sum + " "
        else ""
      }

      println("mst: " + res.mkString)
    }

  }


  def test7 = withOutFileChecked(prefix + "mst7") {
    runMst7(1,true)
  }

  def runMst7(mult: Int, test: Boolean) {

    // fully incremental: use aux maps for partial bid results, 
    // remove bid->ask dependency

    val o = new IncrementalReader; import o._
    o.multiplier = mult
    o.debug = test

    val aMap = new MutableTreeImpl
    val bMap = new MutableTreeImpl

    val aResCountMap = new MutableTreeImpl
    val aResSumMap = new MutableTreeImpl

    val bResCountMapB = new Array[MutableTreeImpl](10)
    val bResSumMapB = new Array[MutableTreeImpl](10)

    for (i <- 0 until 10) {
      bResCountMapB(i) = new MutableTreeImpl
      bResSumMapB(i) = new MutableTreeImpl
    }

    val bResBrokerSumB = new Array[Double](10)

    // ---
    //o.load()
    o.run { b =>
      val bSum2 = bMap.getSum(b.price)
      val bVal3 = bMap.get(b.price)

      bMap.add(b.price, b.volume)

      // update all broker maps (result)
      for (i <- 0 until 10) {
        bResSumMapB(i).shiftKeys(bSum2-bVal3, b.volume) // add volume to existing sums
        bResCountMapB(i).shiftKeys(bSum2-bVal3, b.volume) // add volume to existing sums
      }

      bResSumMapB(b.broker_id.toInt).add(bSum2 + b.volume, b.price * b.volume) // add at new position
      bResCountMapB(b.broker_id.toInt).add(bSum2 + b.volume, b.X) // add at new position

      calc()
    } { a=>
      val aSum2 = aMap.getSum(a.price)
      val aVal3 = aMap.get(a.price)

      aMap.add(a.price, a.volume)

      aResSumMap.shiftKeys(aSum2-aVal3, a.volume) // add volume to existing sums
      aResSumMap.add(aSum2 + a.volume, a.price * a.volume) // add at new position

      aResCountMap.shiftKeys(aSum2-aVal3, a.volume) // add volume to existing sums
      aResCountMap.add(aSum2 + a.volume, a.X) // add at new position

      calc()
    }

    def calc() {

      val aResCount = aResCountMap.getSum - aResCountMap.getSum(0.75 * aMap.getSum)
      val aResSum = aResSumMap.getSum - aResSumMap.getSum(0.75 * aMap.getSum)

      for (i <- 0 until 10) {
        val bResCount = bResCountMapB(i).getSum - bResCountMapB(i).getSum(0.75 * bMap.getSum)
        val bResSum = bResSumMapB(i).getSum - bResSumMapB(i).getSum(0.75 * bMap.getSum)

        bResBrokerSumB(i) = bResCount * aResSum - aResCount * bResSum
      }

      if (debug) {
        val res = (0 until 10).toList.map { i =>
          val sum = bResBrokerSumB(i)        
          if (sum != 0) i.toDouble + ":" + sum + " "
          else ""
        }

        println("mst: " + res.mkString)
      }
    }



    if (!debug) {
      val res = (0 until 10).toList.map { i =>
        val sum = bResBrokerSumB(i)        
        if (sum != 0) i.toDouble + ":" + sum + " "
        else ""
      }

      println("mst: " + res.mkString)
    }

  }

}