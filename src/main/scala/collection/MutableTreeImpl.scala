package baseline.collection

// TODO: support patch operation

final class MutableTreeImpl {

  type K = Double
  type V = Double

  type Color = Boolean

  private final val RED = false
  private final val BLACK = true


  // tree node class

  final class Tree {

    var left: Tree = _
    var right: Tree = _
    var k: K = _
    var v: V = _
    var color: Color = _
    var count: Int = _
    var depth: Int = _
    var minK: K = _
    var maxK: K = _

    var sum: V = _

    override def toString = "["+left+"("+k+","+v+")"+right+"]"

  }

  // Sedgewick's LLRB balancing (extended to handle parent-relative keys)
  // TBD: do we need delete rebalancing?

  private def rotateLeft(h: Tree): Tree = {
    //  before:  y -- h -- (a -- x -- b)
    //  after:   (y -- h -- a) -- x -- b 

    // hkey = base + h.k             = base + x.k' + h.k'  =>  h.k' = h.k - x.k' = - x.k
    // xkey = base + h.k + x.k       = base + x.k'         =>  x.k' = h.k + x.k

    // akey = base + h.k + x.k + a.k = base + x.k' + h.k' + a.k'  =>  a.k' = a.k + x.k

    val x = h.right;
    val hk = h.k
    h.right = x.left;
    h.k = -x.k
    if (x.left ne null) {
      x.left.k = x.left.k + x.k
      aggr(x.left)
    }
    x.left = aggr(h);
    x.color = x.left.color;
    x.left.color = RED;
    x.k = x.k + hk
    aggr(x)
  }

  private def rotateRight(h: Tree): Tree = {
    //  before:  (a -- x -- b) -- h -- y
    //  after:   a -- x -- (b -- h -- y)

    val x = h.left;
    val hk = h.k
    h.left = x.right;
    h.k = -x.k
    if (x.right ne null) {
      x.right.k = x.right.k + x.k
      aggr(x.right)
    }
    x.right = aggr(h);
    x.color = x.right.color;
    x.right.color = RED;
    x.k = x.k + hk
    aggr(x)
  }

  private def flipColors(h: Tree): Unit = {
     h.color = !h.color;
     h.left.color =  !h.left.color;
     h.right.color = !h.right.color;
  }

  private def isRed(h: Tree) = (h != null) && !h.color



  // create new tree containing just (k,v)

  private def newNode(k: K, v: V): Tree = {
    val h = new Tree
    h.k = k
    h.v = v
    h.color = RED
    h.count = 1
    h.depth = 1
    h.minK = k
    h.maxK = k
    h.sum = v
    h
  }

  // (re-)compute aggregate values (sum, ...)

  private def aggr(h: Tree) = {
    h.count = 1
    h.depth = 1
    h.minK = h.k
    h.maxK = h.k
    h.sum = h.v
    var depthL = 0
    var depthR = 0
    if (h.left ne null) {
      h.count += h.left.count
      depthL = h.left.depth
      h.minK = h.left.minK + h.k
      h.sum += h.left.sum
    }
    if (h.right ne null) {
      h.count += h.right.count
      depthR = h.right.depth
      h.maxK = h.right.maxK + h.k
      h.sum += h.right.sum
    }
    h.depth += (if (depthL > depthR) depthL else depthR)
    h
  }


  // insert an item - either replace or add if it already exists

  private def put0(h: Tree, add: Boolean, k: K, v: V): Tree = {

    if (h == null) return newNode(k, v);

    if (isRed(h.left) && isRed(h.right)) flipColors(h);

    if (k == h.k) h.v = if (add) h.v + v else v;
    else if (k < h.k) h.left = put0(h.left, add, k-h.k, v);
    else h.right = put0(h.right, add, k-h.k, v);

    aggr(h)

    if (isRed(h.right) && !isRed(h.left))    return rotateLeft(h);
    if (isRed(h.left) && isRed(h.left.left)) return rotateRight(h);

    return h
  }


  // shift all keys in tree by d

  private def shift0(node: Tree, d: K): Tree = {
    if (node == null) return null
    node.k += d
    node.minK += d
    node.maxK += d
    node
  }



  var log = false
  def lp(x: Any) = if (log) println(x)


  // left and right subtrees are structurally ok, but they may overlap. fix.
  // aggreagtes of h may not be valid (but h.k is)

  private def fixMerge(h: Tree, sel: Int): Tree = {
      //println("merging from left: "+h.left.count+" nodes of "+h.count)

/*
      if (h.right == null) {
        assert(h.left.maxK >= 0)
        println("merging top into left: "+h.left.count+" nodes of "+h.count)
        val k2 = -h.left.k
        return put0(shift0(h.left, h.k), true, k2, h.v)
      }
      if (h.left == null) {
        assert(h.right.minK <= 0)
        println("merging top into right: "+h.right.count+" nodes of "+h.count)
        val k2 = -h.right.k
        return put0(shift0(h.right, h.k), true, k2, h.v)
      }
*/
      // TODO: opt
      if (sel == 0) {
        //h.left != null && h.left.maxK >= h.k) {
        //assert(h.right.minK)
        //println("merging from left: "+h.left.count+" nodes of "+h.count)
        val left2 = h.left
        var h2: Tree = h
        h2.left = null
        aggr(h2)
        foreach0(left2, h.k, { (k1,v1) => 
          h2 = put0(h2, true, k1, v1)
        })
        return h2
      } else {
        //println("merging from right: "+h.right.count+" nodes of "+h.count)
        val right2 = h.right
        var h2: Tree = h
        h2.right = null
        aggr(h2)
        foreach0(right2, h.k, { (k1,v1) => 
          h2 = put0(h2, true, k1, v1)
        })
        return h2
      }

  }


  // shift all keys > k by d

  private def shiftKeys0(h: Tree, k: K, d: K): Tree = { 
    if (h == null) return null

    //if (k == 5) log = true
    //lp(k+" "+d + " "+h)

    if (d < 0) {

      //println("may need to merge at "+ h.k)

      if (k < h.k) {
        h.left = shiftKeys0(h.left, k-h.k, d); shift0(h.left,-d); h.k += d
        if (h.left == null || h.k + h.left.maxK < h.k)
          return aggr(h)

        // left.maxK >= h.k
        return fixMerge(h,0)
      }

      //println("going right at "+ h.k + " " + h.right)
      // k >= h.k

      h.right = shiftKeys0(h.right, k-h.k, d)
      if (h.right == null || h.k < h.right.minK + h.k) 
        return aggr(h)

      // right2.minK <= h.k -- need to merge

      return fixMerge(h,1)

    } else {
      // always safe, never need to merge

      if (k < h.k) { h.left = shiftKeys0(h.left, k-h.k, d); shift0(h.left,-d); h.k += d }
      else { h.right = shiftKeys0(h.right, k-h.k, d) }

      return aggr(h)
    }
  }

  // patch-insert:
  //  split tree at position of key k
  //  shift all keys right from k by d
/*
  private def patch0(h: Tree, k: K, d: K, v: V): Tree = {

    if (h == null) return newNode(k, v);

    if (isRed(h.left) && isRed(h.right)) flipColors(h);

    if (k == h.k) h.v = v;
    else if (k < h.k) { h.left = patch0(h.left, k-h.k, v); h.right = shift0(h.right, d) }
    else h.right = patch0(h.right, k-h.k, v);

    aggr(h)

    if (isRed(h.right) && !isRed(h.left))    return rotateLeft(h);
    if (isRed(h.left) && isRed(h.left.left)) return rotateRight(h);

    return h
  }
*/



  // get i-th element

  def at0(h: Tree, base: K, i: Int): (K,V) = {
    val ct = if (h.left == null) 0 else h.left.count
    if (i == ct) (base + h.k,h.v)
    else if (i < ct) at0(h.left, base + h.k, i)
    else at0(h.right, base + h.k, i - ct - 1)
  }


  // get exact value for key k, 0 if not found

  def getExact0(h: Tree, k: K): V = {
    if (h == null) return 0

    if (k < h.k) getExact0(h.left, k-h.k)
    else if (k == h.k) h.v
    else getExact0(h.right, k-h.k)
  }

  // get value for largest key <= k

  def getLast0(h: Tree, k: K, m: V): V = {
    if (h == null) return m

    if (k < h.k) getLast0(h.left, k-h.k, m)
    else if (k == h.k) h.v
    else getLast0(h.right, k-h.k, h.v)
  }


  // get cumulated sum for keys <= k

  def getSum0(h: Tree, k: K): V = {
    if (h == null) return 0

    if (k > h.maxK) return h.sum

    if (k < h.k) getSum0(h.left, k-h.k)
    else if (k == h.k) getSum0(h.left, k-h.k) + h.v
    else (if (h.left == null) 0.0 else h.left.sum) + h.v + getSum0(h.right, k-h.k)
  }


  // iterate over all elements

  def foreach0(h: Tree, base: K, f: (K,V) => Unit): Unit = {
    if (h == null) return
    foreach0(h.left,base+h.k,f)
    f(base+h.k,h.v)
    foreach0(h.right,base+h.k,f)
  }





  // public api

  var root: Tree = null

  def put(k: K, v: V): Unit = {
    root = put0(root,false,k,v)
    root.color = BLACK
    checkInvariants()
  }

  def add(k: K, v: V): Unit = {
    root = put0(root,true,k,v)
    root.color = BLACK
    checkInvariants()
  }


  def shift(d: K): Unit = {
    root = shift0(root,d)
  }

  def shiftKeys(k: K, d: K): Unit = {
    //println(k+" "+d + " "+root)

    /*println("before")
    foreach { (k,v) => print(k+" ") }
    println*/

    // FIXME: revisit

    root = shiftKeys0(root,k,d)
    if (root ne null) root.color = BLACK
    checkInvariants()

    /*
    if (root == null) return

    val h = root
    root = null

    foreach0(h,List(0), { (k1,v1) => 
      if (k1 <= k) // the = seems important!!!
        add(k1,v1)
      else
        add(k1+d,v1)
    })
    */

    /*println("after " + k+" "+d)
    foreach { (k,v) => print(k+" ") }
    println*/

  }



  def at(i: Int): (K,V) = {
    at0(root,0,i)
  }

  def getSum(k: K): V = {
    getSum0(root,k)
  }

  def getSum: V = {
    if (root == null) 0
    else root.sum
  }


  def get(k: K): V = {
    getExact0(root,k)
  }

  def foreach(f: (K,V) => Unit): Unit = {
    foreach0(root,0,f)
  }


  override def toString = {
    var s = ""; 
    foreach((k,v)=>if (v!=0.0)s+=(k+":"+v+" "));
    s
  }

  def checkInvariants(): Unit = {
    return // debug only
    val x = new scala.collection.mutable.ListBuffer[K]
    foreach { (k,v) => x += k }
    val xs = x.toList
    assert(xs == xs.sorted, xs + "!=" + xs.sorted)
  }


}

