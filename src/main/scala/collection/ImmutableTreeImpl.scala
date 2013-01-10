package baseline.collection

final class ImmutableTreeImpl {

  type K = Double
  type V = Double
  
  // 3 columns:  Index  Key  Value
  //             -----------------
  //               0     k0    v0
  //               1     k1    v1
  //               2     ..    ..
  //
  //  API:
  //  lookup/update by index
  //  lookup/update by key
  //
  //  not sure if index needed if key is numeric?

  type Color = Boolean

  private final val RED = false
  private final val BLACK = true

  abstract class Tree {
    def color: Color
    def count: Int
    def depth: Int
    def sum: V
    def min: V
    def max: V

    def put(k: K, v: V): Tree
    def shift(k: K): Tree


    def at(i: Int): (K,V)
//    def getValue(k: K): V
//    def getSum(k: K): V

    def foreach(f: (K,V) => Unit): Unit
  }

  case class Empty() extends Tree {
    def color = BLACK
    def count = 0
    def depth = 0
    def sum = 0
    def min = Double.MaxValue
    def max = Double.MinValue

    def put(k: K, v: V): Tree = branch(Empty(),Empty(),k,v)
    def shift(k: K): Tree = this

    def at(i: Int): (K,V) = ???

    def foreach(f: (K,V) => Unit): Unit = {}
  }


  case class Branch(left: Tree, right: Tree, k: K, v: V, color: Color, count: Int, depth: Int, sum: V, min: V, max: V) extends Tree {

    def put(k1: K, v1: V): Tree = {

      if (k1 < k) branch(left.put(k1-k,v1), right, k,v)
      else        branch(left, right.put(k1-k,v1), k,v)
    }

    def shift(d: K): Tree = branch(left,right,k+d,v)

    def at(i: Int): (K,V) = {
      assert(i < count)
      val (k1,v1) = if (i < left.count) left.at(i)
      else if (i == left.count) (0.0,v)
      else right.at(i - left.count - 1)
      (k1+k,v1)
    }
    def foreach(f: (K,V) => Unit): Unit = {
      left.foreach((k1,v1) => f(k+k1,v1))
      f(k,v)
      right.foreach((k1,v1) => f(k+k1,v1))
    }

  }

  def branch(l: Tree, r: Tree, k: K, v: V) = {
    Branch(l,r,k,v, RED, l.count+1+r.count, (l.depth max r.depth)+1, l.sum+v+r.sum, l.min min r.min, l.max max r.max)
  }



  var root: Tree = Empty()

  def put(k: K, v: V): Unit = {
    root = root.put(k,v)
  }

  def shift(d: K): Unit = {
    root = root.shift(d)
  }

  def at(i: Int): (K,V) = root.at(i)

  def foreach(f: (K,V) => Unit): Unit = {
    root.foreach(f)
  }


}
