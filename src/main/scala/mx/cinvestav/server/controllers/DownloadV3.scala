package mx.cinvestav.server.controllers

import mx.cinvestav.Declarations.NodeContext
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
import mx.cinvestav.commons.types.{Download, DownloadCompleted, DownloadResult, NodeQueueStats}
import mx.cinvestav.operations.Operations
import mx.cinvestav.commons.utils

import scala.concurrent.duration._
import language.postfixOps
import retry._
import retry.implicits._
import scala.concurrent.duration._
import language.postfixOps

object DownloadV3 {

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root/"download"/"completed" => for {
      arrivalTime <- IO.monotonic.map(_.toNanos)
      _                <- s.acquire
      currentState     <- ctx.state.get
      headers          = req.headers
      nodeId           = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("NODE_ID")
      objectId         = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse("OBJECT_ID")
      operationId      = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse("OPERATION_ID")

      //      queue            = currentState.nodeQueue
      operations        = currentState.operations
      //        queue.getOrElse(nodeId,Nil).sortBy(_.arrivalTime)
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
      //        currentState.completedOperations
      maybeUp          = operations.find(_.operationId == operationId)
      lastCompleted    = completeQueue.maxByOption(_.arrivalTime)
      response         <- maybeUp match {
        case Some(o) =>
          o match {
            case download:Download =>
              val _wt                 = lastCompleted.map(_.departureTime).getOrElse(0L) - download.arrivalTime
              val wt                 = if(_wt>0) _wt else 0

              val downloadCompleted  = DownloadCompleted(
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
//              val nextOp             = nodeQueue.minByOption(_.ser)
//            ____________________________________________________________________
              val saveOp             = ctx.state.update{ s=>
                s.copy(
                  completedQueue      =  s.completedQueue.updatedWith(nodeId) {
                    case op@Some(value) =>
                      op.map(x => x :+ downloadCompleted)
                    case None => (downloadCompleted :: Nil).some
                  },
                  completedOperations =  s.completedOperations :+downloadCompleted,
                  pendingQueue        =  s.pendingQueue.updatedWith(nodeId) {
                    case Some(maybeOperation) => None
                    case None => None
                  },
                  nodeQueue           =  s.nodeQueue.updated(
                    nodeId,
                    s.nodeQueue.getOrElse(nodeId,Nil).filterNot(_.operationId == operationId ))
                )
              }
              for {
                _          <- saveOp
                res        <- NoContent()
                st         = downloadCompleted.serviceTime
                objectSize = download.objectSize
                _          <- ctx.logger.info(s"DOWNLOAD_COMPLETED $operationId $objectId $objectSize $nodeId $st 0 $wt")
                _          <- Operations.nodeNextOperation(nodeId).start
              } yield res
            case _ => NotFound()
          }
        case None =>
          NotFound()
      }
      _                <- ctx.logger.debug("_____________________________________________")
      _                <- s.release
      //      _                <- Operations.nextOperationV2().start
    } yield response

    case req@GET -> Root / "download" / objectId  =>
      val app = for {
        arrivalTime       <- IO.monotonic.map(_.toNanos)
        _                 <- s.acquire
        currentState      <- ctx.state.get
        headers           = req.headers
        clientId          = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        objectSize        = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        technique         = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        clientArrivalTime = headers.get(CIString("Client-Arrival-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        ds                = Operations.distributionSchema(
          operations          = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique           = technique,
          queue               = currentState.nodeQueue
        )
        maybeObject       = ds.get(objectId).flatMap(xs=>Option.when(xs.nonEmpty)(xs))
        queue             = currentState.nodeQueue
        avgSTS            = Operations.getAVGServiceTime(operations= currentState.completedOperations)
        avgWTS            = Operations.getAVGWaitingTimeByNode(completedOperations = currentState.completedQueue,queue = queue )
//      ______________________________________________________________________________________________________________________________________
        (response,download)     <- maybeObject match {
          case Some(replicaNodes) => for {
            _            <- IO.unit
            replicaNodeX = replicaNodes.map(currentState.nodes).map(n=>n.nodeId -> n).toMap
            _            <- ctx.logger.debug(s"REPLICA_NODES ${replicaNodeX.keys.toList}")
            operationId  = utils.generateNodeId(prefix = "op",len = 10,autoId=true)

            selectedNode = if(replicaNodes.length == 1 ) currentState.nodes(replicaNodes.head)
            else Operations.downloadBalance(x = ctx.config.downloadLoadBalancer,replicaNodeX = replicaNodeX )(
              objectId       = objectId,
              operations     = currentState.operations,
              queue          = currentState.nodeQueue,
              completedQueue = currentState.completedQueue,
              objectSize     = objectSize
            )
            selectedNodeId             = selectedNode.nodeId
            _ <- ctx.logger.debug(s"SELECTED_NODE $selectedNodeId")
            completedOpsBySelectedNode = currentState.completedQueue.getOrElse(selectedNodeId,Nil)
            q                          = queue.getOrElse(selectedNodeId,Nil)
            download                   = Download(
              operationId   = operationId,
              serialNumber  = q.length + completedOpsBySelectedNode.length,
              arrivalTime   = arrivalTime,
              objectId      = objectId,
              objectSize    = objectSize,
              clientId      = clientId,
              nodeId        = selectedNodeId,
              metadata      = Map("CLIENT_ARRIVAL_TIME"->clientArrivalTime.toString),
              correlationId = operationId
            )
            _                          <- ctx.state.update{ s=>
              s.copy(
                operations = s.operations :+ download,
                nodeQueue = s.nodeQueue.updated(selectedNodeId,q :+ download)
              )
            }
            result                     = DownloadResult(
              id = utils.generateNodeId(prefix = "download",autoId = true, len = 10),
              result = NodeQueueStats(
                operationId = operationId, nodeId = selectedNodeId,
                avgServiceTime = avgSTS.getOrElse(selectedNodeId,0.0),
                avgWaitingTime = avgSTS.getOrElse(selectedNodeId,0.0),
                queuePosition = q.length,
                objectSize     = objectSize
              )
            )
            response                   <- Ok(result.asJson)
            extra       = if (ctx.config.extraServiceTimeMs <= 1) 0 else (scala.util.Random.nextLong(ctx.config.extraServiceTimeMs*10) + ctx.config.extraServiceTimeMs )
            _           <- IO.sleep(extra milliseconds)
            serviceTime <- IO.monotonic.map(_.toNanos - arrivalTime)
            operationId = download.operationId
            nodeId      = download.nodeId
            avgSt       = avgSTS.getOrElse(nodeId,0L)
            avgWt       = avgWTS.getOrElse(nodeId,0L)
            _           <- ctx.logger.info(s"DOWNLOAD $operationId $objectId $objectSize $nodeId $serviceTime $avgSt $avgWt")
            _           <- ctx.logger.debug("_____________________________________________")
            _           <- Operations.nodeNextOperation(download.nodeId).start
          } yield (response,download)
          case None => for {
            response <- NotFound()
            download = Download.empty
          } yield (response,download)
        }
        _           <- s.release
      } yield response

      app.onError{ e=>
        ctx.logger.error("DOWNLOAD_ERROR "+e.getMessage)
      }
  }


}
