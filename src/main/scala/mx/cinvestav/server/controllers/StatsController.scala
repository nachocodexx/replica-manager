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
      case GET -> Root / "stats" =>
        val program = for {
        _                  <- IO.unit
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
//        distributionSchema = Events.generateDistributionSchemaV2(events = events,ctx.config.replicationTechnique)
//        objectsIds         = Events.getObjectIds(events = events)
//        hitCounter         = Events.getHitCounterByNode(events = events)
//        hitRatioInfo       = Events.getGlobalHitRatio(events=events)
//        tempMatrix         = Events.generateTemperatureMatrixV2(events = events,windowTime = 0)
//        tempMatrix0        = Events.generateTemperatureMatrixV3(events = events,windowTime = 0)
//        replicaUtilization = Events.generateReplicaUtilizationMap(events =events)
        nodeIds            = Events.getNodeIds(events = events)
        arsJson            =  ars.map(x=>x.nodeId->x)
          .toMap
          .asJson
//      ______________________________________________________________________________________________________________________________________________
//        pendingPuts        = EventXOps.onlyPendingPuts(events = events)
//        pendingGets        = EventXOps.onlyPendingGets(events = events)
//        pendingNodes       = (pendingPuts.map(_.nodeId) ++ pendingGets.map(_.nodeId)).distinct

        _ <- IO.unit
        stats              = Map(
          "nodeId" -> ctx.config.nodeId.asJson,
          "port"  -> ctx.config.port.asJson,
//          "ipAddress" -> currentState.ip.asJson,
          "nodes" -> Operations.processNodes(currentState.nodes,operations).toMap.asJson,
//          "availableResources" ->arsJson,
//          "distributionSchema" -> distributionSchema.asJson,
          "objectIds" -> objectsIds.sorted.asJson,
          "nodeIds" -> nodeIds.asJson,
//          "hitCounterByNode"-> hitCounter.asJson,
//          "replicaUtilization" -> replicaUtilization.toArray.asJson,
//          "hitInfo" -> hitRatioInfo.asJson,
          "loadBalancing" -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancerToken.asJson
          ),
          "apiVersion" -> ctx.config.apiVersion.asJson,
          "queues" -> nodesQueue.asJson,
//          "operations" -> operations.asJson,
          "completedQueues" -> completedQueues.asJson,

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
