/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case op: Operation => root ! op
    case GC => {
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => () //already garbage collecting, so ignore other requests
    
    case op: Operation => {
      //add it to the pending queue
      pendingQueue = pendingQueue.enqueue(op)
    }
    
    case CopyFinished => {
      //process the queue
      root ! PoisonPill
      
      while(!pendingQueue.isEmpty) {
        val (message, modifiedQueue) = pendingQueue.dequeue
        pendingQueue = modifiedQueue
        root ! message
      }
      
      pendingQueue = Queue.empty[Operation] //not sure if this is required as we should have removed all ops from the queue
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = { 
    case Insert(requester, id, e) => {
      if (this.elem == e) {
        println("EEEEQQQQUUUAAAAALLLLLLL -> " + elem + " - " + e)
        removed = false
        requester ! OperationFinished(id)
      } else {
        if (e > this.elem) {
          println("insertion is larger than this, go right -> " + e + " this: " + elem)
          if (subtrees.contains(Right)) {
        	 subtrees(Right) ! Insert(requester, id, e)
          } else {
             subtrees = subtrees.updated(Right, context.actorOf(BinaryTreeNode.props(e, initiallyRemoved = false)))
             requester ! OperationFinished(id)
          }
        } else {
           println("insertion is smaller than this, go left -> " + e + " this: " + elem)
          if (subtrees.contains(Left)) {
        	 subtrees(Left) ! Insert(requester, id, e)
          } else {
             subtrees = subtrees.updated(Left, context.actorOf(BinaryTreeNode.props(e, initiallyRemoved = false)))
             requester ! OperationFinished(id)
          }
        }
      }
    }
      
    case Remove(requester, id, e) => {
      if (elem == e) { 
        removed = true
        requester ! OperationFinished(id)
      } else {
        if (this.elem < e && subtrees.contains(Right)) {
          subtrees(Right) ! Remove(requester, id, e)
        } else if (this.elem > e && subtrees.contains(Left)) {
          subtrees(Left) ! Remove(requester, id, e)
        } else {
          requester ! OperationFinished(id)
        }
      }
    }
      
    case Contains(requester, id, e) => {
      if (elem == e) requester ! ContainsResult(id, !removed) 
      else {
	      //pass the message along
	      if (this.elem < e && subtrees.contains(Right)) {
	        //go search right
	        subtrees(Right) ! Contains(requester, id, e)
	      } else if (this.elem > e && subtrees.contains(Left)){
	        //search left
	        subtrees(Left) ! Contains(requester, id, e)
	      } else {
	         requester ! ContainsResult(id, false)
	      }
      }
    }
      
    case CopyTo(newRoot) => {
    	val childrenIter = subtrees.values
    	
    	if (!this.removed) {
    	  newRoot ! Insert(self, 0, elem)
    	  context.become(copying(childrenIter.toSet, false))
    	} else if (childrenIter.isEmpty) {
    	  context.parent ! CopyFinished
    	} else {
    	  context.become(copying(childrenIter.toSet, true))
    	}
    }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(0) => checkFinished(expected, insertConfirmed)
    case CopyFinished => checkFinished(expected - sender, insertConfirmed)
  }
  
  def checkFinished(expected: Set[ActorRef], insertConfirmed: Boolean): Unit = {
    if(expected.isEmpty && insertConfirmed) {
      context.unbecome()
      context.parent ! CopyFinished
      subtrees.values.foreach { _ ! PoisonPill }
    } else {
      context.become(copying(expected, insertConfirmed))
    }
  }

}
