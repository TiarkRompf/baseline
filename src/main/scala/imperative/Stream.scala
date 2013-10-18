package baseline.imperative

// streams

object Stream {
  def apply[A] = new Stream[A,A] {
    def into[B](out: StreamFunc[A,B]): StreamFunc[A,B] = out
  }
}

abstract class Stream[A,B] { self =>
  def into[C](out: StreamFunc[B,C]): StreamFunc[A,C]

  // consumer api
  def print = into(StreamFunc.print)
  def collect = into(StreamFunc.collect)
  def sum(implicit ev: Numeric[B]) = into(StreamFunc.sum)
  def groupBy[K,C](sel: B => K)(inner: Stream[B,B] => StreamFunc[B,C]) = into(StreamFunc.groupBy(sel, inner(Stream[B])))
  def groupBy1[K,C](sel: B => K,inner: Stream[B,B] => StreamFunc[B,C]) = into(StreamFunc.groupBy(sel, inner(Stream[B])))

  // transformer api
  def tally(implicit ev: Numeric[B]) = new Stream[A,B] {
    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.tally(out))
  }
  def filter(p: B => Boolean) = new Stream[A,B] {
    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.filter(p)(out))
  }
  def takeWhile(p: B => Boolean) = new Stream[A,B] {
    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.takeWhile(p, out))
  }
  def map[B1](f: B => B1) = new Stream[A,B1] {
    def into[C](out: StreamFunc[B1,C]) = self.into(StreamFunc.map(f)(out))
  }
  def flatMap[B1](f: B => Generator[B1]) = new Stream[A,B1] {
    def into[C](out: StreamFunc[B1,C]) = self.into(StreamFunc.flatMap(f)(out))
  }

  // binary transformer api
  def zip[A1,B1](other: Stream[A1,B1]) = new Stream[Either[A,A1],(B,B1)] {
    def into[C](out: StreamFunc[(B,B1),C]) = {
      val (left,right) = StreamFunc.zip(out)
      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
    }
  }
  def cartesian[A1,B1](other: Stream[A1,B1])= new Stream[Either[A,A1],(B,B1)] {
    def into[C](out: StreamFunc[(B,B1),C]) = {
      val (left,right) = StreamFunc.cartesian(out)
      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
    }
  }
  def join[K,A1,B1](other: Stream[A1,B1])(selA: B => K, selB: B1 => K) = new Stream[Either[A,A1],(B,B1)] {
    def into[C](out: StreamFunc[(B,B1),C]) = {
      val (left,right) = StreamFunc.join(selA, selB, out)
      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
    }
  }
}