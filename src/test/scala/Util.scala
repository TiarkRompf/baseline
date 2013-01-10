package baseline


import baseline.collection._

import java.io.PrintWriter
import scala.collection._


class BulkReader {

  case class Record(t: Double, id: Long, broker_id: Long, volume: Double, price: Double, X: Int)

  val bids = new mutable.ArrayBuffer[Record]
  val asks = new mutable.ArrayBuffer[Record]

  var deletePhysically = false
  var multiplier = 1
  var debug = false

  def run(calc: => Unit): Unit = {

    import baseline.imperative._
    import org.dbtoaster._
    import org.dbtoaster.StreamAdaptor._;
    import org.dbtoaster.Source._;
    import org.dbtoaster.dbtoasterExceptions._;
    import org.dbtoaster.ImplicitConversions._;
    import org.dbtoaster.StdFunctions._;

    def dispatchStream(e: DBTEvent) = {      
      dispatchStream0(e)
      if (debug) println(e)
      calc
    }

    def dispatchStream0(e: DBTEvent) = e match {
      case StreamEvent(InsertTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID,var_BIDS_VOLUME,var_BIDS_PRICE, 1);
        bids += x

      case StreamEvent(DeleteTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        if (deletePhysically) {
          val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID, var_BIDS_VOLUME,var_BIDS_PRICE, 1);
          bids -= x
        } else {
          val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID, - var_BIDS_VOLUME,var_BIDS_PRICE, -1);
          bids += x
        }

      case StreamEvent(InsertTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID,var_ASKS_VOLUME,var_ASKS_PRICE, 1);
        asks += x

      case StreamEvent(DeleteTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        if (deletePhysically) {
          val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID, var_ASKS_VOLUME,var_ASKS_PRICE, 1);
          asks -= x
        } else {
          val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID, - var_ASKS_VOLUME,var_ASKS_PRICE, -1);
          asks += x
        }

      case StreamEvent(SystemInitialized, o, "", Nil) => //onSystemInitialized();

      case EndOfStream => ();

      case _ => throw DBTFatalError("Event could not be dispatched: " + e)
    }

    val dataAdaptor = new StreamOp[String] {
      val bda = createAdaptor("orderbook", "BIDS", List(("book", "bids"),("brokers", "10"),("deterministic", "yes")))
      val ada = createAdaptor("orderbook", "ASKS", List(("book", "asks"),("brokers", "10"),("deterministic", "yes")))
      def +=(x: String) = { bda.processTuple(x).foreach(dispatchStream); ada.processTuple(x).foreach(dispatchStream) }
      def isReady = true
      def eof() = {}
    }
    
    for (i <- 0 until multiplier)
      Generator.file("data/finance.csv").into(dataAdaptor)

  }
}


class IncrementalReader {

  case class Record(t: Double, id: Long, broker_id: Long, volume: Double, price: Double, X: Int)

  var multiplier = 1
  var debug = false

  def run(onBid: Record => Unit)(onAsk: Record => Unit): Unit = {

    import baseline.imperative._
    import org.dbtoaster._
    import org.dbtoaster.StreamAdaptor._;
    import org.dbtoaster.Source._;
    import org.dbtoaster.dbtoasterExceptions._;
    import org.dbtoaster.ImplicitConversions._;
    import org.dbtoaster.StdFunctions._;

    def dispatchStream(e: DBTEvent) = {      
      if (debug) println(e)
      dispatchStream0(e)
    }

    def dispatchStream0(e: DBTEvent) = e match {
      case StreamEvent(InsertTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID,var_BIDS_VOLUME,var_BIDS_PRICE, 1);
        onBid(x)

      case StreamEvent(DeleteTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID, - var_BIDS_VOLUME,var_BIDS_PRICE, -1);
        onBid(x)

      case StreamEvent(InsertTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID,var_ASKS_VOLUME,var_ASKS_PRICE, 1);
        onAsk(x)

      case StreamEvent(DeleteTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID, - var_ASKS_VOLUME,var_ASKS_PRICE, -1);
        onAsk(x)

      case StreamEvent(SystemInitialized, o, "", Nil) => //onSystemInitialized();

      case EndOfStream => ();

      case _ => throw DBTFatalError("Event could not be dispatched: " + e)
    }

    val dataAdaptor = new StreamOp[String] {
      val bda = createAdaptor("orderbook", "BIDS", List(("book", "bids"),("brokers", "10"),("deterministic", "yes")))
      val ada = createAdaptor("orderbook", "ASKS", List(("book", "asks"),("brokers", "10"),("deterministic", "yes")))
      def +=(x: String) = { bda.processTuple(x).foreach(dispatchStream); ada.processTuple(x).foreach(dispatchStream) }
      def isReady = true
      def eof() = {}
    }
    
    for (i <- 0 until multiplier)
      Generator.file("data/finance.csv").into(dataAdaptor)

  }
}


class BulkIncrementalReader {

  case class Record(t: Double, id: Long, broker_id: Long, volume: Double, price: Double, X: Int)

  var multiplier = 1
  var debug = false

  val bids = new mutable.ArrayBuffer[Record]
  val asks = new mutable.ArrayBuffer[Record]

  def load(): Unit = {

    import baseline.imperative._
    import org.dbtoaster._
    import org.dbtoaster.StreamAdaptor._;
    import org.dbtoaster.Source._;
    import org.dbtoaster.dbtoasterExceptions._;
    import org.dbtoaster.ImplicitConversions._;
    import org.dbtoaster.StdFunctions._;

    def dispatchStream(e: DBTEvent) = {      
      if (debug) println(e)
      dispatchStream0(e)
    }

    def dispatchStream0(e: DBTEvent) = e match {
      case StreamEvent(InsertTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID,var_BIDS_VOLUME,var_BIDS_PRICE, 1);
        bids += x
        asks += null

      case StreamEvent(DeleteTuple, o, "BIDS", (var_BIDS_T: Double)::(var_BIDS_ID: Long)::(var_BIDS_BROKER_ID: Long)::(var_BIDS_VOLUME: Double)::(var_BIDS_PRICE: Double)::Nil) => 
        val x = Record(var_BIDS_T,var_BIDS_ID,var_BIDS_BROKER_ID, - var_BIDS_VOLUME,var_BIDS_PRICE, -1);
        bids += x
        asks += null

      case StreamEvent(InsertTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID,var_ASKS_VOLUME,var_ASKS_PRICE, 1);
        bids += null
        asks += x

      case StreamEvent(DeleteTuple, o, "ASKS", (var_ASKS_T: Double)::(var_ASKS_ID: Long)::(var_ASKS_BROKER_ID: Long)::(var_ASKS_VOLUME: Double)::(var_ASKS_PRICE: Double)::Nil) => 
        val x = Record(var_ASKS_T,var_ASKS_ID,var_ASKS_BROKER_ID, - var_ASKS_VOLUME,var_ASKS_PRICE, -1);
        bids += null
        asks += x

      case StreamEvent(SystemInitialized, o, "", Nil) => //onSystemInitialized();

      case EndOfStream => ();

      case _ => throw DBTFatalError("Event could not be dispatched: " + e)
    }

    val dataAdaptor = new StreamOp[String] {
      val bda = createAdaptor("orderbook", "BIDS", List(("book", "bids"),("brokers", "10"),("deterministic", "yes")))
      val ada = createAdaptor("orderbook", "ASKS", List(("book", "asks"),("brokers", "10"),("deterministic", "yes")))
      def +=(x: String) = { bda.processTuple(x).foreach(dispatchStream); ada.processTuple(x).foreach(dispatchStream) }
      def isReady = true
      def eof() = {}
    }
    
    for (i <- 0 until multiplier)
      Generator.file("data/finance.csv").into(dataAdaptor)

  }

  def run(onBid: Record => Unit)(onAsk: Record => Unit): Unit = {
    val n = bids.length
    var i = 0
    while (i < n) {
      val b = bids(i)
      if (b != null) onBid(b)
      else onAsk(asks(i))
      i += 1
    }
  }

}
