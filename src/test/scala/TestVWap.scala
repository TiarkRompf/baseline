package baseline


import baseline.collection._

import java.io.PrintWriter
import scala.collection._



class TestVWap extends FileDiffSuite {

  val prefix = "test-out/test12-"

  def test1 = withOutFileChecked(prefix + "vwap1") {

    val o = new BulkReader; import o._
    o.debug = false
    o.multiplier = 2
    o.run { /* callback not used here */ }

    println("size: " + bids.size)


    // keep two independent tree indices, recalc sum at each step
    var resbuf = new mutable.ArrayBuffer[(MutableTreeImpl,MutableTreeImpl,Double)]

    var buf = new mutable.ArrayBuffer[Record]
    bids.foreach { b =>
      buf += b
      val bids = buf
      println(b)

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

        val resC = sum1m.root.sum - sum1m.getSum(0.75 * sum3)

      println("sumC:  " + resC)

      resbuf += ((sum2m, sum1m, resC))

    }


    println("----------")


    // fully incremental, single loop

    val resC2 =  {
      var sum3 = 0.0
      val sum2m = new MutableTreeImpl
      val sum1m = new MutableTreeImpl

      var i = 0

      bids.foreach { b2 => 

        println(b2)

        sum3 += b2.volume

        val s2 = sum2m.getSum(b2.price)
        val s3 = sum2m.get(b2.price)

        println("price "+b2.price+" volume "+b2.volume+" shift "+s2+"-"+s3+" >> "+b2.volume+" add "+(s2+b2.volume)+" + "+(b2.price*b2.volume))

        sum1m.shiftKeys(s2-s3, b2.volume)

        sum1m.add(s2 + b2.volume, b2.price * b2.volume) // add at new position

        sum2m.add(b2.price, b2.volume)

        val resC2 = sum1m.root.sum - sum1m.getSum(0.75 * sum3)

        // done calculating, now check

        val (expSum2m, expSum1m, expResC) = resbuf(i)

        print("expect: "); val e2 = expSum2m.toString; println(e2)
        print("got:    "); val g2 = sum2m.toString; println(g2)
        print("expect: "); val e1 = expSum1m.toString; println(e1)
        print("got:    "); val g1 = sum1m.toString; println(g1)
        println("sumC2: " + resC2)

        assert(e2 == g2, "g2")
        assert(e1 == g1, "g1")

        if (expResC != resC2) {          
          println("error: expected sum " + expResC)
          assert(false)
        }

        i += 1
      }

    }


  }
}