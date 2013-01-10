package baseline


import baseline.collection._

import java.io.PrintWriter
import scala.collection._



object RunVWap {

  def main(args: Array[String]): Unit = {

    def time[A](s:String,n:Int=20)(x: => A): A = List.range(0,n).map { _ =>
      val t0 = System.nanoTime
      val r = x
      val t1 = System.nanoTime
      println(s+": " + (t1-t0)/1000.0 + " us")
      r
    }.last;



    val o = new BulkReader
    o.multiplier = 1
    import o._

    time("file to buffer",1) {
      o.run() // callback not used
    }

    println("size: " + bids.size)


    // just use lists and nested loops
/*
    val resA = time("set to sumA ",5) {
      val sum3 = bids.map(b3 => b3.volume).sum
      val sum = bids.filter { b1 =>
        val sum2 = bids.filter(b2 => b2.price > b1.price).map(b2 => b2.volume).sum
        (0.25 * sum3 > sum2)
      }.map(b1 => b1.price * b1.volume).sum
      sum
    }

    println("sumA:  " + resA)*/


    // build a tree index, remove nested loops

    val resB = time("buffer to sumB ") {
      var sum3 = 0.0; bids.foreach{ b3 => sum3 += b3.volume }

      val sum2m = new MutableTreeImpl
      bids.foreach { b2 => sum2m.add(b2.price, b2.volume) }

      def sum2f(b1_price: Double) = sum2m.root.sum - sum2m.getSum(b1_price)

      var sum = 0.0; bids.foreach { b1 =>
        if (0.25 * sum3 > sum2f(b1.price))
          sum += b1.price * b1.volume
      }

      sum
    }

    println("sumB:  " + resB)

    // optimize a little

    val resB2 = time("buffer to sumB2") {
      var sum3 = 0.0
      val sum2m = new MutableTreeImpl
      var sum = 0.0

      bids.foreach { b2 => 

        sum3 += b2.volume
        sum2m.add(b2.price, b2.volume) 
      }

      bids.foreach { b1 =>
        if (sum2m.getSum(b1.price) > 0.75 * sum3)
          sum += b1.price * b1.volume
      }

      sum
    }
    println("sumB2: " + resB2)



    // add a second tree index

    val resC = time("buffer to sumC ") {
      var sum3 = 0.0
      val sum2m = new MutableTreeImpl

      bids.foreach { b2 => 

        sum3 += b2.volume
        sum2m.add(b2.price, b2.volume) 

      }

      val sum1m = new MutableTreeImpl

      bids.foreach { b1 =>
        sum1m.add(sum2m.getSum(b1.price), b1.price * b1.volume)
      }

      sum1m.root.sum - sum1m.getSum(0.75 * sum3)
    }

    println("sumC:  " + resC)



    // fully incremental, single loop

    val resC2 = time("buffer to sumC2") {
      var sum3 = 0.0
      val sum2m = new MutableTreeImpl
      val sum1m = new MutableTreeImpl

      var i = 0

      println("size: " + bids.length)

      bids.foreach { b2 => 

        sum3 += b2.volume

        val s2 = sum2m.getSum(b2.price)
        val s3 = sum2m.get(b2.price)

        sum1m.shiftKeys(s2-s3, b2.volume) // take sum before element as index

        sum1m.add(s2 + b2.volume, b2.price * b2.volume) // add at new position

        sum2m.add(b2.price, b2.volume)

        // todo: could fuse ops per tree
      }

      sum1m.root.sum - sum1m.getSum(0.75 * sum3)
    }

    println("sumC2: " + resC2)



    {val o = new IncrementalReader; import o._

    val resC2 = time("file to sumC2") {
      var sum3 = 0.0
      val sum2m = new MutableTreeImpl
      val sum1m = new MutableTreeImpl

      var resC2 = 0.0

      o.run { b2 => 

        sum3 += b2.volume

        val s2 = sum2m.getSum(b2.price)
        val s3 = sum2m.get(b2.price)

        sum1m.shiftKeys(s2-s3, b2.volume) // take sum before element as index

        sum1m.add(s2 + b2.volume, b2.price * b2.volume) // add at new position

        sum2m.add(b2.price, b2.volume)

        resC2 = sum1m.root.sum - sum1m.getSum(0.75 * sum3)
        // todo: could fuse ops per tree
      } { a => }

      resC2
    }

    println("sumC2: " + resC2)}

  }
}