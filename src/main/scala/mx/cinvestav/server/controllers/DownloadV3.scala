package mx.cinvestav.server.controllers

import mx.cinvestav.Declarations.NodeContext
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.typelevel.ci.CIString
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.commons.types.{Download, DownloadCompleted, DownloadResult, NodeQueueStats, Upload, UploadCompleted}
import mx.cinvestav.operations.Operations
import mx.cinvestav.commons.utils

import scala.concurrent.duration._
import language.postfixOps
import retry._
import retry.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef

import scala.concurrent.duration._
import language.postfixOps

object DownloadV3 {

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root/"download" /"completed" => for {
      arrivalTime <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      headers          = req.headers
      nodeId           = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("NODE_ID")
      objectId         = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse("OBJECT_ID")
      operationId      = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse("OPERATION_ID")
      queue            = currentState.nodeQueue
      nodeQueue        = queue.getOrElse(nodeId,Nil).sortBy(_.serialNumber)
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
      maybeUp          = nodeQueue.find(_.operationId == operationId)
      lastCompleted    = completeQueue.maxByOption(_.arrivalTime)
      response         <- maybeUp match {
        case Some(o) =>
          o match {
            case download:Download =>
              val wt  = lastCompleted.map(_.departureTime).getOrElse(0L) - download.arrivalTime
              val completed  = DownloadCompleted(
                operationId  = download.operationId,
                serialNumber = download.serialNumber,
                arrivalTime  = arrivalTime,
                serviceTime  = arrivalTime - download.arrivalTime,
                waitingTime  = if(wt < 0L ) 0L else wt,
                idleTime     = if(wt < 0L) wt*(-1) else 0L,
                objectId     = objectId,
                nodeId       = nodeId,
                metadata     = Map.empty[String,String] ++ download.metadata,
                objectSize   = download.objectSize
              )

              val nextOp    = nodeQueue.minByOption(_.serialNumber)
              val saveOp = ctx.state.update{ s=>
                s.copy(
                  completedQueue      =  s.completedQueue.updatedWith(nodeId) {
                    case op@Some(value) =>
                      op.map(x => x :+ completed)
                    case None => (completed :: Nil).some
                  },
                  completedOperations =  s.completedOperations :+completed,
                  pendingQueue        =  s.pendingQueue.updated(nodeId,nextOp),
                  nodeQueue           =  s.nodeQueue.updated(nodeId,s.nodeQueue.getOrElse(nodeId,Nil).filterNot(_.operationId == operationId ))
                )
              }
              for {
                _   <- saveOp
                res <- NoContent()
              } yield res
            case _ => NotFound()
          }
        case None => NotFound()
      }
    } yield response
    case req@GET -> Root / "download" / objectId  =>
      val app = for {
        arrivalTime <- IO.monotonic.map(_.toNanos)
        _ <- s.acquire
        currentState <- ctx.state.get
        headers      = req.headers
        clientId     = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        objectSize   = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        technique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        ds           = Operations.distributionSchema(
          operations          = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique           = technique
        )
        maybeObject  = ds.get(objectId).flatMap(xs=>Option.when(xs.nonEmpty)(xs))
        queue        = currentState.nodeQueue
        avgSTS       = Operations.getAVGServiceTime(operations= currentState.completedOperations)
        avgWTS       = Operations.getAVGWaitingTimeByNode(completedOperations = currentState.completedQueue,queue = queue )
        response     <- maybeObject match {
          case Some(replicaNodes) => for {
            _            <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
            replicaNodeX = replicaNodes.map(currentState.nodes).map(n=>n.nodeId -> n).toMap
            operationId  = utils.generateNodeId(prefix = "op-",len = 10,autoId=true)
            selectedNode = if(replicaNodes.length == 1 ) currentState.nodes(replicaNodes.head)

            else Operations.downloadBalance(x = ctx.config.downloadLoadBalancer,nodexs = replicaNodeX )(
              objectId  = objectId,
              operations = currentState.operations,
              queue=currentState.nodeQueue,
              completedQueue = currentState.completedQueue,
              objectSize = objectSize
            )
            selectedNodeId = selectedNode.nodeId
            _              <- ctx.logger.debug(s"SELECTED_NODE $selectedNodeId")
            q              = queue.getOrElse(selectedNodeId,Nil)

            download       = Download(
              operationId   = operationId,
              serialNumber  = q.length,
              arrivalTime   = arrivalTime,
              objectId      = objectId,
              objectSize    = objectSize,
              clientId      = clientId,
              nodeId        = selectedNodeId,
              metadata      = Map.empty[String,String],
              correlationId = operationId
            )
            _              <- ctx.state.update{ s=>
              s.copy(
                operations = s.operations :+ download,
                nodeQueue = s.nodeQueue.updated(selectedNodeId,q :+ download)
              )
            }

            result = DownloadResult(
              id = utils.generateNodeId(prefix = "download",autoId = true),
              result = NodeQueueStats(
                operationId = operationId, nodeId = selectedNodeId,
                avgServiceTime = avgSTS.getOrElse(selectedNodeId,0.0),
                avgWaitingTime = avgSTS.getOrElse(selectedNodeId,0.0),
                queuePosition = q.length
              )
            )

            response <- Ok(result.asJson)

          } yield response
          case None => NotFound()
        }

        //      EXTRA SERVICE TIME
        extra = (scala.util.Random.nextLong(ctx.config.extraServiceTimeMs)  + scala.util.Random.nextLong(1000L))
        _      <- IO.sleep(extra milliseconds)
        //
        _ <- s.release
      } yield response

      app.onError{ e=>
        ctx.logger.error(e.getMessage)
      }
  }


}
