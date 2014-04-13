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

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
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

  var pendingQueue = Queue.empty[Operation]

  def receive = normal

  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case _ =>
  }

  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => pendingQueue = pendingQueue enqueue op
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    }
    case _ =>
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def receive = normal

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem) => insert(requester, id, elem)
    case Contains(requester, id, elem) => contains(requester, id, elem)
    case Remove(requester, id, elem) => remove(requester, id, elem)
    case CopyTo(newRoot) => copyTo(newRoot)
    case _ =>
  }

  def insert(requester: ActorRef, id: Int, elem: Int) = elem match {
    case this.elem =>
      removed = false; requester ! OperationFinished(id)
    case _ => {
      val insertPosition = if (this.elem > elem) Right else Left 
      val subtree = subtrees.getOrElse(insertPosition, createSubtree(insertPosition, elem))
      subtree ! Insert(requester, id, elem)
    }
  }

  def contains(requester: ActorRef, id: Int, elem: Int) = elem match {
    case this.elem => if (removed) requester ! ContainsResult(id, false) else requester ! ContainsResult(id, true)
    case _ => {
      val subtree = getSubtree(elem)
      subtree match {
        case Some(st) => st ! Contains(requester, id, elem)
        case None => requester ! ContainsResult(id, false)
      }
    }
  }

  def remove(requester: ActorRef, id: Int, elem: Int) = elem match {
    case this.elem =>
      removed = true; requester ! OperationFinished(id)
    case _ => {
      val subtree = getSubtree(elem)
      subtree match {
        case Some(st) => st ! Remove(requester, id, elem)
        case None => requester ! OperationFinished(id)
      }
    }
  }

  def createSubtree(position: Position, elem: Int) = {
    val subtree = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
    subtrees = subtrees + (position -> subtree)
    subtree
  }

  def getSubtree(elem: Int) =
    if (this.elem > elem) subtrees.get(Right) else subtrees.get(Left)

  def copyTo(newRoot: ActorRef) = {
    if (removed && subtrees.isEmpty) context.parent ! CopyFinished
    else {
      if (!removed) newRoot ! Insert(self, -1, elem)
      subtrees.values.foreach { _ ! CopyTo(newRoot) }
      context.become(copying(subtrees.values.toSet, removed))
    }
  }

  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(-1) => {
      if (expected.isEmpty) context.parent ! CopyFinished
      else context.become(copying(expected, true))
    }
    case CopyFinished => {
      val expectedUpdated = expected - sender
      if (insertConfirmed && expectedUpdated.isEmpty) context.parent ! CopyFinished
      else context.become(copying(expectedUpdated, insertConfirmed))
    }
  }

}
