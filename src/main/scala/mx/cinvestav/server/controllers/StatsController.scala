package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.operations.Operations
import org.typelevel.ci.CIString
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.events.Events
//
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object StatsController {


  def apply()(implicit ctx:NodeContext): HttpRoutes[IO] = {

    HttpRoutes.of[IO]{

      case req@GET -> Root / "stats" / "operations" => for {
        currentState  <- ctx.state.get
        x             = currentState.operations.groupBy(_.nodeId).asJson
        response      <- Ok(x)
      } yield response

      case req@GET -> Root / "stats" / "operations" / "completed" => for {
        currentState    <- ctx.state.get
        completedQueues = currentState.completedQueue
        x               = completedQueues.map{
          case (nodeId,xs) =>
            nodeId-> xs.map(_.asJson(completedOperationEncoder))
        }.toMap.asJson
        response      <- Ok(x)
      } yield response

      case req@GET -> Root / "stats" / "queue" => for {
        currentState <- ctx.state.get
        x            = currentState.nodeQueue
        response     <- Ok(x.asJson)
      } yield response

      case req@GET -> Root / "stats" / "queue" / "completed" => for {
        currentState <- ctx.state.get
        x            = currentState.completedQueue
        response     <- Ok(x.asJson)
      } yield response

      case req@GET -> Root / "stats" =>
        val program = for {
          _                  <- IO.unit
          headers    = req.headers
          technique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
          currentState       <- ctx.state.get
          nodesQueue         = currentState.nodeQueue
          ds = Operations.distributionSchema(
          operations = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique = technique,
            queue = currentState.nodeQueue
        )

          avgServiceTime      = Operations.getAVGServiceTime(operations = currentState.completedOperations)
          nodes               = currentState.nodes
          completedOperations = currentState.completedOperations
          ops                 = currentState.operations
          completedQueue      = currentState.completedQueue

          processedN           = Operations.processNodes(
          nodexs = nodes,
          completedOperations = completedOperations,
          queue = nodesQueue,
          operations = ops
        ).toMap
          downloads        = Operations.onlyDownload(ops)
          uploads          = Operations.onlyUpload(ops)
//        ______________________________________________________________________________________________________________
          upsByNodeId      = uploads.groupBy(_.nodeId)
          dwsByNodeId      = downloads.groupBy(_.nodeId)
          opsByNode        = ops.groupBy(_.nodeId)
//        ______________________________________________________________________________________________________________
          iats       = Operations.avgInterarrival(queue = nodesQueue)
          globalIats = Operations.avgInterarrival( queue =  opsByNode )
          upIats     = Operations.avgInterarrival(queue = upsByNodeId)
          dwIats     = Operations.avgInterarrival(queue = dwsByNodeId)
          //        ______________________________________________________________________________________________________________
          iarts      = Operations.avgInterarrivalRate(queue = nodesQueue)

          globalSts  = Operations.getAVGServiceTimeNodeIdXCOps(completedQueue)
          globalWTS  = Operations.getAVGWaitingTimeByNode(completedOperations = completedQueue,queue = currentState.nodeQueue)
          ls         = Operations.avgOperationsInQueue(avgInterArrivalRate = iarts,avgWaitingTime = globalWTS)
          _ <- IO.unit
          stats      = Map(
            "nodeId"                      -> ctx.config.nodeId.asJson,
            "port"                        -> ctx.config.port.asJson,
            "nodes"                       -> processedN.asJson,
            "loadBalancing"               -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancerToken.asJson
          ),
            "apiVersion"                  -> ctx.config.apiVersion.asJson,
            "avgServiceTime"              -> avgServiceTime.asJson,
            "distributionSchema"          -> ds.asJson,
            "totalAvgWaitingTimesByNode"  -> Operations.getAVGWaitingTimeNodeIdXCOps(completedQueue).asJson,
            "avgServiceTimesByNode"       -> globalSts.asJson,
            "avgWaitingTimesByNode"       -> globalWTS.asJson,
            "accessByBall"                -> Operations.ballAccess(completedOperations = currentState.completedOperations).asJson,
            "acessByNode"                 -> Operations.ballAccessByNodes(
            completedOperations = completedQueue,
            operations = ops
          ).asJson,
            "avgInterarrival"             -> iats.asJson,
            "avgInterarrivalRate"         -> iarts.asJson,
            "serverUtilization"           -> Operations.serverUtilization(interArrivals = iats,serviceTimes = globalSts ,parallelServers = processedN.size).asJson,
            "avgOperationsInQueue"        -> ls.asJson,
            "ballAccessByNode"            -> Operations.ballAcessByNode(nodeIds  = processedN.keys.toList,completedOperations = currentState.completedQueue).asJson,
            "globalIATS"                  -> globalIats.asJson,
            "operationsInService"         -> currentState.pendingQueue.map{
            case (nodeId,op) =>
              val x = op match {
                case Some(value) => value.asJson
                case None => Json.Null
              }
              nodeId ->  x
          }.asJson,
            "totalDownloads"              -> Operations.onlyDownloadCompleted(completedOperations).length.asJson,
            "totalUploads"                -> Operations.onlyUploadCompleted(completedOperations).length.asJson,
            "totalEnqueues"               -> currentState.nodeQueue.values.toList.length.asJson
          )
          response <- Ok(stats)
      } yield response

        program.handleErrorWith{ e =>
          val errorMsg = e.getMessage
          val headers  = Headers(Header.Raw(CIString("Error-Message"), errorMsg ) )

          ctx.logger.error(errorMsg) *> InternalServerError(errorMsg,headers)
        }
    }

  }

}
