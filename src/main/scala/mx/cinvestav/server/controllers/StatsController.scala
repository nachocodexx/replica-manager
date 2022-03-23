package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{EventXOps, UpdatedNodePort}
import org.typelevel.ci.CIString

import scala.collection.immutable.ListMap
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
        currentState       <- ctx.state.get
        rawEvents          = currentState.events
        events             = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        ars                = EventXOps.getAllNodeXs(events=events).map { node =>
              val nodeId = node.nodeId
              val publicPort = Events.getPublicPort(events= events,nodeId).map(_.publicPort).getOrElse(6666)
            node.copy(
              metadata = node.metadata ++ Map("PUBLIC_PORT"->publicPort.toString)
            )
        }
        distributionSchema = Events.generateDistributionSchema(events = events)
        objectsIds         = Events.getObjectIds(events = events)
        hitCounter         = Events.getHitCounterByNode(events = events)
        hitRatioInfo       = Events.getGlobalHitRatio(events=events)
        tempMatrix         = Events.generateTemperatureMatrixV2(events = events,windowTime = 0)
        tempMatrix0        = Events.generateTemperatureMatrixV3(events = events,windowTime = 0)
        replicaUtilization = Events.generateReplicaUtilization(events =events)
        nodeIds            = Events.getNodeIds(events = events)
        arsJson            =  ars.map(x=>x.nodeId->x)
          .toMap
          .asJson
//      ________________________________________________________
        serviceTimeByNode  = Events.getAvgServiceTimeByNode(events=events)
        stats              = Map(
          "nodeId"                   -> ctx.config.nodeId.asJson,
          "port"  -> ctx.config.port.asJson,
          "ipAddress" -> currentState.ip.asJson,
          "availableResources" ->arsJson,
          "distributionSchema" -> distributionSchema.asJson,
          "objectIds" -> objectsIds.sorted.asJson,
          "nodeIds" -> nodeIds.asJson,
          "hitCounterByNode"-> hitCounter.asJson,
          "tempMatrix" -> tempMatrix.toArray.asJson,
          "tempMatrix0" -> tempMatrix0.toArray.asJson,
          "replicaUtilization" -> replicaUtilization.toArray.asJson,
          "hitInfo" -> hitRatioInfo.asJson,
          "loadBalancing" -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancer.map(_.token).getOrElse(ctx.config.uploadLoadBalancer).asJson
          ),
          "serviceTimes" -> serviceTimeByNode.asJson,
          "apiVersion" -> ctx.config.apiVersion.asJson
        )
        response <- Ok(stats)
      } yield response

        program.handleErrorWith{ e =>
          val errorMsg = e.getMessage()
          val headers  = Headers(Header.Raw(CIString("Error-Message"), errorMsg ) )

          ctx.logger.error(errorMsg) *> InternalServerError(errorMsg,headers)
        }
    }

  }

}
