package baseline


import baseline.collection._

import java.io.PrintWriter
import scala.collection._



object RunMst {

  def main(args: Array[String]): Unit = {

    def time[A](s:String,n:Int=20)(x: => A): A = List.range(0,n).map { _ =>
      val t0 = System.nanoTime
      val r = x
      val t1 = System.nanoTime
      println(s+": " + (t1-t0)/1000.0 + " us")
      r
    }.last;

    val mult = 1

    time("loading") {
      val o = new IncrementalReader
      o.multiplier = mult
      o.run(_=>())(_=>())
    }


    val alg = new TestMst

    time("mst") {
      alg.runMst7(mult, false)
    }

  }
}