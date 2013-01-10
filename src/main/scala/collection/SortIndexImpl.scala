package baseline.collection

final class SortIndexImpl[@specialized A:Ordering](inverse: Int=>A) {
  
  var permutation = new Array[Int](0)
  var permutation2 = new Array[Int](0)
  var sz = 0
  
  def +=(x: A) = {
    sz += 1
  }
  
  private def ensurePermutation() = // should just merge ... inefficient to always build new array
    if (permutation.length < sz) {
      permutation = Array.range(0,sz).sortBy(inverse)
      permutation2 = new Array(sz)
      for (i <- 0 until sz)
        permutation2(permutation(i)) = i
    }
  
  def get(i: Int) = {
    ensurePermutation()
    permutation(i)
  }
  
  def rank(i: Int) = {
    ensurePermutation()
    permutation2(i)
  }  
  
}