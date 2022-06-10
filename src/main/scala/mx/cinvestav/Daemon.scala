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
import mx.cinvestav.operations.Operations.nextOperation

import scala.concurrent.duration._
import scala.language.postfixOps
import retry._
import retry.implicits._

object Daemon {

  def apply(period:FiniteDuration = 1 second )(implicit ctx:NodeContext)= {
    val s0 = Stream.awakeEvery[IO]( period = period).evalMap{ _ =>
      for {
        currentState <- ctx.state.get
        _ <- nextOperation(
          nodexs = currentState.nodes.values.toList,
          queue = currentState.nodeQueue,
          pending = currentState.pendingQueue
        )
//        _ <- ctx.logger.debug("_____________________________________________--")
      } yield ()
    }

    val s1 = Stream.awakeEvery[IO](period = period).evalMap{ _ =>
      for {
        currentState <- ctx.state.get
        wts      = Operations.getAVGWaitingTimeByNode(completedOperations = currentState.completedQueue,queue = currentState.nodeQueue)
        wtss     = wts.map(_._2)
        globalWT = wtss.sum / wtss.size.toDouble
        qSNodes  = wts.filter(_._2 > globalWT).keys
        _ <- ctx.logger.debug(s"GLOBAL_WT $globalWT")
        _ <- ctx.logger.debug(s"QSNodes $qSNodes")
//        fs =
      } yield()
    }
    s0 concurrently s1
  }
}
