package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{EventXOps, Get, Put}
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
      case req@GET -> Root / "stats" =>
        val program = for {
        _                  <- IO.unit
        headers    = req.headers
        technique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        currentState       <- ctx.state.get
        pendingReplicas    = currentState.pendingReplicas.filter(_._2.rf>0)
        rawEvents          = currentState.events
        events             = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        nodesQueue         = currentState.nodeQueue
        operations         = currentState .operations
        completedQueues    = currentState.completedQueue
        ars                = EventXOps.getAllNodeXs(events=events).map { node =>
              val nodeId = node.nodeId
              val publicPort = Events.getPublicPort(events= events,nodeId).map(_.publicPort).getOrElse(6666)
            node.copy(
              metadata = node.metadata ++ Map("PUBLIC_PORT"->publicPort.toString)
            )
        }
        ds = Operations.distributionSchema(
          operations = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique = technique
        )
        nodeIds            = Events.getNodeIds(events = events)

        avgServiceTime = Operations.getAVGServiceTime(operations = currentState.completedOperations)
        _ <- IO.unit
        stats              = Map(
          "nodeId" -> ctx.config.nodeId.asJson,
          "port"  -> ctx.config.port.asJson,
          "nodes" -> Operations.processNodes(
            nodexs = currentState.nodes,
            completedOperations = currentState.completedOperations,
            queue = nodesQueue
          ).toMap.asJson,
          "nodeIds" -> nodeIds.asJson,
          "loadBalancing" -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancerToken.asJson
          ),
          "apiVersion" -> ctx.config.apiVersion.asJson,
          "queues" -> nodesQueue.asJson,
          "completedQueues" -> completedQueues.asJson,
          "avgServiceTime" -> avgServiceTime.asJson,
          "distributionSchema" -> ds.asJson,
          "avgWaitingTimesByNode"  -> Operations.getAVGWaitingTimeNodeIdXCOps(currentState.completedQueue).asJson,
          "avgServiceTimesByNode"  -> Operations.getAVGServiceTimeNodeIdXCOps(currentState.completedQueue).asJson

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
