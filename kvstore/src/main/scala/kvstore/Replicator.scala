package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case op: Replicate => {
      println("RECEIVED A REPLICATE REQUEST")
      val snapshot = Snapshot(op.key, op.valueOption, _seqCounter)
      
      //takes the key and value and creates a new Snapshot message to send to its replica
      acks = acks + (_seqCounter -> (sender, op))
      pending = pending :+ (snapshot)
      context.system.scheduler.schedule(0 millis, 250 millis, replica, snapshot)
      nextSeq
    }
    case op: SnapshotAck => {
      //waits for snapshotAck to be returned
      //sends a response back with success
      val storedSender = acks(op.seq)
      storedSender._1 ! Replicated(op.key, storedSender._2.id)
    }
  }

}
