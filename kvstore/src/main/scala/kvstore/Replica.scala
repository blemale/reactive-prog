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
import akka.actor.ActorLogging
import akka.actor.Cancellable

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

  private object ResendUnackedPersistence
  private case class PersistenceTimeout(id: Long)
  private case class GlobalTimeout(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import scala.language.postfixOps

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // persistence service
  val persistenceService = context.actorOf(persistenceProps)

  // key-value map
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // map from operation id to sender, persistence message and persistence timeout 
  var persistenceAcks = Map.empty[Long, (ActorRef, Persist, Option[Cancellable])]
  var replicationAcks = Map.empty[Long, (ActorRef, Cancellable, Set[ActorRef])]
  // actual sequence index
  var actualSeq = 0

  // scheduler for send again unacked persistence message every 100 milliseconds 
  context.system.scheduler.schedule(100 milliseconds,
    100 milliseconds,
    self,
    ResendUnackedPersistence)

  // join the cluster
  arbiter ! Join

  override val supervisorStrategy = {
    import akka.actor.SupervisorStrategy._
    OneForOneStrategy() {
      case _: PersistenceException ⇒ Restart // restart persistence service if persistence fails
      case _: Exception ⇒ Escalate
    }
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // Behavior for  the leader role //
  val leader: Receive = {
    case op: Operation => handleOperation(op)
    case Persisted(key, id) => handlePersistenceAckPrimary(id)
    case ResendUnackedPersistence => resendUnackedPersistence()
    case PersistenceTimeout(id) => handleTimeout(id)
    case Replicated(key, id) => handleReplicated(key, id)
    case GlobalTimeout(id) => handleTimeout(id)
    case Replicas(replicas) => handleReplicas(replicas)
  }

  def handleReplicated(key: String, id: Long) = {
    replicationAcks.get(id).foreach {
      case (client, timeout, replicas) =>
        val updatedReplicas = replicas - sender
        if (updatedReplicas.isEmpty) {
          timeout.cancel
          replicationAcks -= id
          if (!persistenceAcks.contains(id)) client ! OperationAck(id)
        } else {
          replicationAcks += id -> (client, timeout, updatedReplicas)
        }
    }
  }

  // Behavior for the replica role //
  val replica: Receive = {
    case snapshot: Snapshot => handleSnapshot(snapshot)
    case Get(key, id) => sendValue(key: String, id: Long)
    case Persisted(key, seq) => handlePersistenceAckSecondary(key, seq)
    case ResendUnackedPersistence => resendUnackedPersistence()
  }

  // Operation //
  def handleOperation(op: Operation) = op match {
    case Insert(key, value, id) =>
      kv += key -> value; handlePersistencePrimary(key, Some(value), id); handleReplication(key, Some(value), id)
    case Remove(key, id) =>
      kv -= key; handlePersistencePrimary(key, None, id); handleReplication(key, None, id)
    case Get(key, id) => sendValue(key: String, id: Long)
  }

  def handleReplication(key: String, value: Option[String], id: Long) {
    if (!replicators.isEmpty) {
      val timeout = context.system.scheduler.scheduleOnce(1 second, self, GlobalTimeout(id))
      replicationAcks += id -> (sender, timeout, replicators)
      for (replicator <- replicators) {
        replicator ! Replicate(key, value, id)
      }
    }
  }

  def sendValue(key: String, id: Long) = {
    val v = kv.get(key)
    sender ! GetResult(key, v, id)
  }

  // Snapshot //
  def handleSnapshot(s: Snapshot): Unit = s match {
    case s @ Snapshot(key, Some(value), _) => handleSnapshotAndUpdateKv(s) { kv += key -> value }
    case s @ Snapshot(key, None, _) => handleSnapshotAndUpdateKv(s) { kv -= key }
  }

  def handleSnapshotAndUpdateKv(s: Snapshot)(kvUpdate: => Unit): Unit =
    ifSeqValid(s.seq) {
      handleValidSnapshot(s)(kvUpdate)
    } {
      handleObsoleteSnapshot(s)
    }

  def ifSeqValid(seq: Long)(validSeqBlock: => Unit)(obsoleteSeqBlock: => Unit) =
    if (seq == actualSeq) {
      validSeqBlock
    } else if (seq < actualSeq) obsoleteSeqBlock

  def handleValidSnapshot(s: Snapshot)(kvUpdate: => Unit) = {
    actualSeq += 1
    kvUpdate
    handlePersistenceSecondary(s.key, s.valueOption, s.seq)
  }

  def handleObsoleteSnapshot(s: Snapshot) = sender ! SnapshotAck(s.key, s.seq)

  // Persistence // 
  def handlePersistencePrimary(key: String, valueOption: Option[String], id: Long) = {
    val timeout = context.system.scheduler.scheduleOnce(1 second, self, PersistenceTimeout(id))
    handlePersistence(key, valueOption, id, Some(timeout))
  }

  def handlePersistenceSecondary(key: String, valueOption: Option[String], id: Long) = {
    handlePersistence(key, valueOption, id, None)
  }

  def handlePersistence(key: String, valueOption: Option[String], id: Long, timeoutOption: Option[Cancellable]) = {
    val message = Persist(key, valueOption, id)
    persistenceAcks += id -> (sender, message, timeoutOption)
    persistenceService ! message
  }

  def handlePersistenceAckPrimary(id: Long) = {
    persistenceAcks.get(id).foreach {
      case (client, _, timeoutOption) =>
        timeoutOption.foreach(_.cancel)
        if (!replicationAcks.contains(id)) client ! OperationAck(id)
    }
    persistenceAcks -= id
  }

  def handlePersistenceAckSecondary(key: String, seq: Long) = {
    persistenceAcks.get(seq).foreach { case (client, _, _) => client ! SnapshotAck(key, seq) }
    persistenceAcks -= seq
  }

  def resendUnackedPersistence() = persistenceAcks.foreach { case (_, (sender, message, _)) => persistenceService ! message }

  def handleTimeout(id: Long) = {
    val sender = persistenceAcks.get(id).map { case (sender, _, _) => sender }
      .orElse(replicationAcks.get(id).map { case (sender, _, _) => sender })
    sender.foreach(_ ! OperationFailed(id))
    persistenceAcks -= id
    replicationAcks -= id
  }

  // Replicas and replicators
  def handleReplicas(replicas: Set[ActorRef]) = {
    val filteredReplicas = replicas.filter(_ != self)
    val newReplicas = filteredReplicas.diff(secondaries.keySet)
    val exitedReplicas = secondaries.keySet.diff(filteredReplicas)
    handleNewReplicas(newReplicas)
    handleExitedeplicas(exitedReplicas)
  }

  def handleNewReplicas(newReplicas: Set[ActorRef]) = {
    for (replica <- newReplicas) {
      val replicator = context.actorOf(Replicator.props(replica))
      secondaries += replica -> replicator
      replicators += replicator
      var id = 0
      kv.foreach {
        case (key, value) => replicator ! Replicate(key, Some(value), id); id += 1
      }
    }
  }

  def handleExitedeplicas(exitedReplicas: Set[ActorRef]) = {
    replicationAcks = replicationAcks.map { case (id, (client, timeout, replicas)) => (id, (client, timeout, replicas -- exitedReplicas.map(secondaries(_)))) }
    for ((id, (client, timeout, replicas)) <- replicationAcks) {
      if (replicas.isEmpty) {
        client ! OperationAck(id)
        replicationAcks -= id
      }
    }
    for (replica <- exitedReplicas) {
      val replicator = secondaries.get(replica)
      secondaries -= replica
      replicator.foreach { r =>
        r ! PoisonPill
        replicators -= r
      }
    }
  }
}
