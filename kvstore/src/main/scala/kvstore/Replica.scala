package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  arbiter ! Join
  val persistence = context.actorOf(persistenceProps)
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  var expectedSeq = 0L
  
  var persistAcks = Map.empty[Long, ActorRef]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case op: Insert => {
      kv = kv + (op.key -> op.value)
      sender ! OperationAck(op.id)
    // now send a replicate message to all the secondaries	

    } 
    case op: Remove => {
      kv = kv - op.key
      sender ! OperationAck(op.id)
      // now send a replicate message to all the secondaries	
    }
    case op: Get => {
      sender ! GetResult(op.key, kv.get(op.key), op.id)
    }
    case op: Replicas => {
      //create a new Replicator from each replica and add to the replicators set and update the secondaries map
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case op: Get => {
      sender ! GetResult(op.key, kv.get(op.key), op.id)
    }
    case op: Persisted => {
      val origSender = persistAcks(op.id)
      origSender ! SnapshotAck(op.key, op.id)
    }
    case op: Snapshot => {
      println("RECEIVED A SNAPSHOT OP: " + op.key + " "  + op.seq + " expected seq= " + expectedSeq)
      //update the KV store.
      val seqNum = op.seq
      op.seq match {
        case x if x == expectedSeq => {
          
          //if we get what we expect then we process it.
          op.valueOption match {
            case Some(v) => {
              kv = kv + (op.key -> v)
              persistAcks = persistAcks + (op.seq -> sender)
              context.system.scheduler.schedule(0 millis, 100 millis, persistence, Persist(op.key, op.valueOption, op.seq))
              expectedSeq = expectedSeq + 1
            }
            case None => {
              kv = kv - op.key
              context.system.scheduler.schedule(0 millis, 100 millis, persistence, Persist(op.key, op.valueOption, op.seq))
              expectedSeq = expectedSeq + 1
            }
          }
        }
        case x if x < expectedSeq => {
          //we've already seen this update, so we send an immediate ack
          sender ! SnapshotAck(op.key, op.seq)
        }
        case x if x > expectedSeq => {
          //do nothing, let the sender timeout and be forced to resend.
        }
      }
    }
    
  }

}
