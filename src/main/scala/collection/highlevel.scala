package baseline.collection

class HashSet[A:ClassManifest] {
  val data = new BufferImpl[A]
  val index = new HashIndexImpl(data(_))
  
  def +=(x: A): this.type = {
    if (index.focus(x) == -1) {
      data += x
      index.putValue(data.size-1)
    }
    this
  }
  
  def contains(x: A): Boolean = index.get(x) != -1  
}

class HashMap[A:ClassManifest, B:ClassManifest] {
  val keys = new BufferImpl[A]
  val values = new BufferImpl[B]
  val index = new HashIndexImpl(keys(_))
  
  def update(k: A, v: B): this.type = {
    val idx = index.focus(k)
    if (idx == -1) {
      keys += k
      values += v
      index.putValue(keys.size-1)
    } else {
      values(idx) = v
    }
    this
  }
  
  def get(k: A): Option[B] = {
    val idx = index.get(k)
    if (idx == -1) None
    else Some(values(idx))
  }
  
}


class RankedList[A:ClassManifest:Ordering] {
  val data = new BufferImpl
  val sorted = new SortIndexImpl[A](data(_))
  val indexMin = new HashIndexImpl(data(_))
  val index = new HashIndexImpl(data(_))
  def +=(x: A) = {
    indexMin.putIfAbsent(x, data.size)
    index.put(x, data.size)
    data += x
    sorted += x
  }
  def get(i: Int) = data(sorted.get(i))
  def rank(x: A) = (sorted.rank(indexMin.get(x)), sorted.rank(index.get(x)))
}
