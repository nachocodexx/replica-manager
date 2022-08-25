package mx.cinvestav

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import fs2.Stream
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{AddedNode, EventX, EventXOps, Get, Put, PutCompleted}
import mx.cinvestav.events.Events
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration
import mx.cinvestav.commons.types.{NodeX, PendingReplication, QueueInfo}
import mx.cinvestav.server.controllers.{DownloadControllerV2, UploadControllerV2}
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import java.util.UUID
import breeze._
import breeze.linalg.DenseVector
import breeze.stats.{mean, median}
import mx.cinvestav.commons.Implicits._
import mx.cinvestav.operations.Operations
//import mx.cinvestav.operations.Operations.nextOperation

import scala.concurrent.duration._
import scala.language.postfixOps
import retry._
import retry.implicits._

object Daemon {

  def apply(period:FiniteDuration = 1 second )(implicit ctx:NodeContext)= {
//    val s0 = Stream.awakeEvery[IO]( period = period).evalMap{ _ =>
//      for {
//        currentState <- ctx.state.get
////      ________________________________________________________________________________________________________________
//        _            <- ctx.logger.debug(s"QUEUE_SIZE ${currentState.nodeQueue.values.flatten.toList.length}")
//        _            <- nextOperation (
//          nodexs  = currentState.nodes.values.toList,
//          queue   = currentState.nodeQueue,
//          pending = currentState.pendingQueue
//        )
////      ________________________________________________________________________________________________________________
//      } yield ()
//    }

    val s1 = Stream.awakeEvery[IO](period = period).evalMap{ _ =>
      for {
        startTime           <- IO.monotonic.map(_.toNanos)
        currentState        <- ctx.state.get
        completedQueue      = currentState.completedQueue
        completedOperations = currentState.completedOperations
        nodeQueue           = currentState.nodeQueue
        nodes               = currentState.nodes
        ops                 = currentState.operations
        wts                 = Operations.getAVGWaitingTimeByNode(completedOperations = completedQueue,queue = nodeQueue)
        wtss                = wts.values.toList
        globalWT            = wtss.sum / wtss.size.toDouble
        qSNodes             = wts.filter(_._2 > globalWT).keys.toList
//      _________________________________________________________________
        nodexs              = Operations.processNodes(
          nodexs = nodes.filter(x=> qSNodes.contains(x._1)),
          completedOperations = completedOperations,
          queue = nodeQueue,
          operations = ops
        ).toMap
//      _________________________________________________________________
        ballAccessByNode    = Operations.ballAcessByNode(
          nodeIds  = nodexs.keys.toList,
          completedOperations = completedQueue
        )
        _                  <- ctx.logger.debug(s"GLOBAL_WT $globalWT")
        _                  <- ctx.logger.debug(s"QSNODES $qSNodes")
        endTime            <- IO.monotonic.map(_.toNanos)

        serviceTime        = endTime - startTime
        _                  <- ctx.logger.info(s"OVERHEAD OPERATION_ID OBJECT_ID 0 NODE_ID $serviceTime 0 0")
        _                  <- ctx.logger.debug("________________________________________________")
      } yield()
    }
    if(ctx.config.replicationDaemon)  s1 else Stream.empty.covary[IO]
//    if(ctx.config.replicationDaemon) s0 concurrently s1 else s0
  }
}
