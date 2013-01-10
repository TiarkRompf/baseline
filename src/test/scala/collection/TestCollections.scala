package baseline.collection

import baseline.FileDiffSuite
import java.io.PrintWriter

import scala.collection._



class TestCollections extends FileDiffSuite {

  val prefix = "test-out/test12-"

  def test1 = withOutFileChecked(prefix + "collections1") {
    val xs = new HashSet[String]
    "ABCDEFGJGGSZAFSHGABKJKST".toList.foreach(xs += _.toString)
    println(xs.data.toArray.mkString(","))
    //println(xs.index.indices.mkString(","))
    println(xs.index.get("A"))
    println(xs.index.get("B"))
    println(xs.index.get("C"))
    println(xs.index.get("D"))
    println(xs.index.get("E"))
    println(xs.index.get("F"))
    println(xs.index.get("G"))
  }
  
  def test2 = withOutFileChecked(prefix + "collections2") {
    val xs = new HashMap[String,Int]
    "ABCDEFGJGGSZAFSHGABKJKST".toList.foreach(c => xs(c.toString) = c.toInt)
    println(xs.keys.toArray.mkString(","))
    println(xs.values.toArray.mkString(","))
    println(xs.index.get("A"))
    println(xs.index.get("B"))
    println(xs.index.get("C"))
    println(xs.index.get("D"))
    println(xs.index.get("E"))
    println(xs.index.get("F"))
    println(xs.index.get("G"))
  }

  def test3 = withOutFileChecked(prefix + "collections3") {
    val xs = new RankedList[String]
    "ABCDEFGJGGSZAFSHGABKJKST".toList.foreach(xs += _.toString)
    println(xs.data.toArray.mkString(","))
    println(xs.data.toArray.map(xs rank _).mkString(","))
    println((0 until xs.data.size).map(xs.sorted get _).mkString(","))
    println(xs.rank("A"))
    println(xs.rank("B"))
    println(xs.rank("C"))
    println(xs.rank("D"))
    println(xs.rank("E"))
    println(xs.rank("F"))
    println(xs.rank("G"))
  }

  def test4 = withOutFileChecked(prefix + "collections4") {
    val xs = new MutableTreeImpl

    xs.put(10,15)
    xs.put(15,10)
    xs.put(30,20)

    xs.put(12,55)

    println(xs.root)

    xs.foreach((k,v)=>println(k+","+v))

    println("at index 2: " + xs.at(2))

    xs.shift(20)


    println(xs.root)

    xs.foreach((k,v)=>println(k+","+v))

    println("at index 2: " + xs.at(2))


  }


  def test5 = withOutFileChecked(prefix + "collections5") {
    val xs = new MutableTreeImpl

    for (i <- 0 until 1000)
        xs.put(i,i)

    xs.foreach((k,v)=>println(k+"->"+v+" ")); println

    println("depth: " + xs.root.depth)
    println("at index 500: " + xs.at(500))
    
    xs.shift(20)

    xs.foreach((k,v)=>println(k+"->"+v+" ")); println

    println("depth: " + xs.root.depth)
    println("at index 500: " + xs.at(500))

    xs.shiftKeys(250, 300)

    xs.foreach((k,v)=>println(k+"->"+v+" ")); println


    println("depth: " + xs.root.depth)
    println("at index 249: " + xs.at(249))
    println("at index 250: " + xs.at(250))
    println("at index 251: " + xs.at(251))
    println("at index 500: " + xs.at(500))


  }




}