package baseline.collection

final class HashIndexImpl[@specialized K](indsz: Int, inverse: Int=>K) {
  private val loadfactor_d2 = 0.4f / 2
  private var indices = Array.fill[Int](nextPow2(indsz))(-1) // TODO: assert(isPow2(indsz))
  private var relbits = Integer.numberOfTrailingZeros(indices.length / 2)

  def this(inverse: Int=>K) = this(128, inverse)

  @inline private def nextPow2(x: Int) = {
    var c = x - 1;
    c |= c >>>  1;
    c |= c >>>  2;
    c |= c >>>  4;
    c |= c >>>  8;
    c |= c >>> 16;
    c + 1;
  }
  
  var sz = 0
  def size = sz

  def matchKey(currelem: Int, k: K) = inverse(currelem) == k

  // return index for key (-1 if not found)
  def get(k: K): Int = {
    val hc = k.## * 0x9e3775cd
    val relbits0 = relbits
    var pos = (hc >>> (32 - relbits0)) * 2
    var currelem = indices(pos)
    var currhash = indices(pos + 1)
    
    val mask = indices.length - 1
    while (currelem != -1 && (currhash != hc || !matchKey(currelem, k))) {
      pos = (pos + 2) & mask
      currelem = indices(pos)
      currhash = indices(pos + 1)
    }
    
    currelem
  }
  
  
  // add a key (maybe overwrite), return its index
  def put(k: K, v: Int): Int = {
    val hc = k.## * 0x9e3775cd
    val relbits0 = relbits
    var pos = (hc >>> (32 - relbits0)) * 2
    var currelem = indices(pos)
    var currhash = indices(pos + 1)
    
    val mask = indices.length - 1
    while (currelem != -1 && (currhash != hc || !matchKey(currelem, k))) {
      pos = (pos + 2) & mask
      currelem = indices(pos)
      currhash = indices(pos + 1)
    }
    
    if (currelem == -1) {
      sz += 1
    }

    indices(pos) = v
    indices(pos + 1) = hc
    
    currelem
  }

  def putIfAbsent(k: K, v: Int): Int = {
    val hc = k.## * 0x9e3775cd
    val relbits0 = relbits
    var pos = (hc >>> (32 - relbits0)) * 2
    var currelem = indices(pos)
    var currhash = indices(pos + 1)
    
    val mask = indices.length - 1
    while (currelem != -1 && (currhash != hc || !matchKey(currelem, k))) {
      pos = (pos + 2) & mask
      currelem = indices(pos)
      currhash = indices(pos + 1)
    }
    
    if (currelem == -1) {
      sz += 1
      indices(pos) = v
      indices(pos + 1) = hc
      v
    } else {
      currelem
    }
  }
  
  var lastpos = -1
  var lasthc = -1
  var lastelem = -1
  
  def focus(k: K): Int = {
    val hc = k.## * 0x9e3775cd
    val relbits0 = relbits
    var pos = (hc >>> (32 - relbits0)) * 2
    var currelem = indices(pos)
    var currhash = indices(pos + 1)
    
    val mask = indices.length - 1
    while (currelem != -1 && (currhash != hc || !matchKey(currelem, k))) {
      pos = (pos + 2) & mask
      currelem = indices(pos)
      currhash = indices(pos + 1)
    }
    
    lastpos = pos
    lasthc = hc
    lastelem = currelem
    
    currelem
  }
  
  def putValue(v: Int): Unit = {
    indices(lastpos) = v
    indices(lastpos + 1) = lasthc
    if (lastelem == -1) sz += 1
    lastpos = -1
    lasthc = -1
    lastelem = -1
  }
  
  
  private def grow() = if (sz > (loadfactor_d2 * indices.length)) {
    val nindices = Array.fill[Int](indices.length * 2)(-1)
    relbits = Integer.numberOfTrailingZeros(nindices.length / 2)
    val mask = nindices.length - 1
    
    // copy indices
    var i = 0
    val relbits0 = relbits
    while (i < indices.length) {
      val elem = indices(i)
      if (elem != -1) {
        val hash = indices(i + 1)
        var pos = (hash >>> (32 - relbits0)) * 2
        
        // insert it into nindices
        var currelem = nindices(pos)
        var currhash = nindices(pos + 1)
        while (currelem != -1) {
          pos = (pos + 2) & mask
          currelem = nindices(pos)
          currhash = nindices(pos + 1)
        }
        nindices(pos) = elem
        nindices(pos + 1) = hash
      }
      i += 2
    }
    
    indices = nindices
  }

}
