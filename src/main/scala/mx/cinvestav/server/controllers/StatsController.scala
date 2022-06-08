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
        response <- Ok()
      } yield response
      case req@GET -> Root / "stats" =>
        val program = for {
        _                  <- IO.unit
        headers    = req.headers
        technique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        currentState       <- ctx.state.get
        nodesQueue         = currentState.nodeQueue
        operations         = currentState .operations
        completedQueues    = currentState.completedQueue
        ds = Operations.distributionSchema(
          operations = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique = technique
        )

        avgServiceTime = Operations.getAVGServiceTime(operations = currentState.completedOperations)
        processedN = Operations.processNodes(
          nodexs = currentState.nodes,
          completedOperations = currentState.completedOperations,
          queue = nodesQueue,
          operations = currentState.operations
        ).toMap

        stats              = Map(
          "nodeId" -> ctx.config.nodeId.asJson,
          "port"  -> ctx.config.port.asJson,
          "nodes" -> processedN.asJson,
          "loadBalancing" -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancerToken.asJson
          ),
          "apiVersion" -> ctx.config.apiVersion.asJson,
          "queues" -> nodesQueue.asJson,
          "completedQueues" -> completedQueues.map{
            case (nodeId,xs) =>
              print(nodeId,xs.map(_.objectId))
              nodeId-> xs.map(_.asJson(completedOperationEncoder))
          }.toMap.asJson,
//            .asJson,
          "avgServiceTime" -> avgServiceTime.asJson,
          "distributionSchema" -> ds.asJson,
          "totalAvgWaitingTimesByNode"  -> Operations.getAVGWaitingTimeNodeIdXCOps(currentState.completedQueue).asJson,
          "avgServiceTimesByNode"  -> Operations.getAVGServiceTimeNodeIdXCOps(currentState.completedQueue).asJson,
          "avgWaitingTimesByNode"  -> Operations.getAVGWaitingTimeByNode(completedOperations = currentState.completedQueue,queue = currentState.nodeQueue).asJson,
          "accessByBall" -> Operations.ballAccess(completedOperations = currentState.completedOperations).asJson,
          "acessByNode" -> Operations.ballAccessByNodes(completedOperations = currentState.completedQueue).asJson,
          "operationsByNode" -> currentState.operations.groupBy(_.nodeId).asJson
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
