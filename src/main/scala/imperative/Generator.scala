package baseline.imperative


// generators

abstract class Generator[A] { self =>
  def into[B](out: StreamFunc[A,B]): B

  // consumer api
  def sum(implicit ev: Numeric[A]) = into(StreamFunc.sum)

  // transformer api 
  def filter(p: A => Boolean) = new Generator[A] {
    def into[B](out: StreamFunc[A,B]): B = self.into(Stream[A].filter(p).into(out))
  }
  def takeWhile(p: A => Boolean) = new Generator[A] {
    def into[B](out: StreamFunc[A,B]): B = self.into(Stream[A].takeWhile(p).into(out))
  }
  def map[A1](f: A => A1) = new Generator[A1] {
    def into[B](out: StreamFunc[A1,B]): B = self.into(Stream[A].map(f).into(out))
  }
  def flatMap[A1](f: A => Generator[A1]) = new Generator[A1] {
    def into[B](out: StreamFunc[A1,B]): B = self.into(Stream[A].flatMap(f).into(out))
  }
}

object Generator {
  def numbers = new Numbers()
  def const[A](c: A) = new Const(c)
  def file(name: String) = new File(name)
  def apply[A](xs: Iterable[A]) = new Collection(xs)
}

class Numbers extends Generator[Int] {
  def into[B](out: StreamFunc[Int,B]): B = {
    var i = 0
    while (out.isReady) {
      out += i
      i += 1
    }
    out.result
  }  
}

class Const[A](c: A) extends Generator[A] {
  def into[B](out: StreamFunc[A,B]): B = {
    while (out.isReady) {
      out += c
    }
    out.result
  }  
}

class Collection[A](xs: Iterable[A]) extends Generator[A] {
  def into[B](out: StreamFunc[A,B]): B = {
    val it = xs.iterator
    while (out.isReady && it.hasNext) {
      out += it.next
    }
    if (!it.hasNext)
      out.eof()
    out.result
  }
}

class File(name: String) extends Generator[String] {
  def into[B](out: StreamFunc[String,B]): B = {
    val delim = "\n"
    val inputStream = new java.io.FileInputStream(name)
    val scanner = new java.util.Scanner(inputStream).useDelimiter(delim)

    while (out.isReady && scanner.hasNextLine()) {
      out += scanner.nextLine()      
    }
    if (!scanner.hasNextLine)
      out.eof()
    out.result
  }
}
