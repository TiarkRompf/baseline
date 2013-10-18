package baseline.imperative

import baseline.FileDiffSuite
import java.io.PrintWriter

import scala.collection._



class TestStreams extends FileDiffSuite {

  val prefix = "test-out/test12-"

  def test1 = withOutFileChecked(prefix + "streams1") {

    implicit def iterable2Gen[T](xs: Iterable[T]) = Generator(xs)


    def evenOdd(x: Int) = if (x % 2 == 0) "even" else "odd"

    val reducer = Stream[Int] filter (_ > 0) groupBy1 (evenOdd, _.sum)

    val result = List(-1,-2,-3,1,2,3,4,5,6) into reducer

    assert(result === Map("odd" -> 9, "even" -> 12))

    println(result)

  }
}