package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.UpdatedNodePort

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
      case GET -> Root / "stats" => for {
        currentState       <- ctx.state.get
        rawEvents          = currentState.events
        events             = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        ars                = EventXOps.getAllNodeXs(events=events)
          .map {
            node =>
              val nodeId = node.nodeId
              val ops        = Events.getOperationsByNodeId(nodeId = nodeId, events = rawEvents)
              val publicPort = Events.getPublicPort(events= events,nodeId).map(_.publicPort).getOrElse(6666)
              val gets = ops.count(_.eventType == "GET")
              val puts = ops.count(_.eventType == "PUT")
              node.copy(
                metadata = Map(
                  "TOTAL_REQUESTS" -> (gets + puts).toString,
                  "DOWNLOAD_REQUESTS" -> gets.toString,
                  "UPLOAD_REQUESTS" -> puts.toString,
                  "PUBLIC_PORT" -> publicPort.toString
                )
              )
          }

        distributionSchema = Events.generateDistributionSchema(events = events)
        objectsIds         = Events.getObjectIds(events = events)
        hitCounter         = Events.getHitCounterByNode(events = events)
        hitRatioInfo       = Events.getGlobalHitRatio(events=events)
        tempMatrix         = Events.generateTemperatureMatrixV2(events = events,windowTime = 0)
        tempMatrix0         = Events.generateTemperatureMatrixV3(events = events,windowTime = 0)
        replicaUtilization = Events.generateReplicaUtilization(events =events)
        nodeIds            = Events.getNodeIds(events = events)
//        hostPorts          = Events.getAllUpdatedNodePort(events=events)
//        hostPortsMap       = Events.getAllUpdatedNodePort(events=events).map(_.asInstanceOf[UpdatedNodePort]).map(x=>x.nodeId->x.newPort).toMap
//        defaultNodesPorts  = ars.filterNot(x=>hostPortsMap.contains(x.nodeId)).map{ x=>
//          x.nodeId -> x.port
//        }.toMap
//        hostPortAndDefaultMap =  hostPortsMap.concat(defaultNodesPorts)
//        hostPortsJson      = hostPortAndDefaultMap
//          .map{
//            case (nodeId, hostPort) => Json.obj(
//              nodeId -> Json.obj(
//                "hostPort"-> hostPort.asJson
//              )
//            )
//          }.foldLeft(Json.Null)((x,y)=>x.deepMerge(y))
//          .toMap.asJson
//        updatedNodePorts   = Events.getAllUpdatedNodePort(events)
//        memoryUf           = Events.calculateMemoryUFByNode(events=events,objectSize = 0).map{
//            case (nodeId, uf) =>
//              Json.obj(
//                nodeId ->Json.obj(
//                  "memoryUF" -> uf.asJson
//                )
//              )
//            //              nodeId -> "uf" -> uf
//          }
        //          .foldLeft(Json.Null)((x,y)=>x.deepMerge(y))

        ufs                = Events.calculateUFs(events=events)
          .map{
            case (nodeId, uf) =>
                Json.obj(
                  nodeId ->Json.obj(
                      "diskUF" -> uf.asJson
                  )
                )
//              nodeId -> "uf" -> uf
          }.foldLeft(Json.Null)((x,y)=>x.deepMerge(y))
//        _ <- ctx.logger.debug(memoryUf.toString)
        arsJson =  ars.map(x=>(x.nodeId->x))
          .toMap
          .asJson
          .deepMerge(ufs)
//          .deepMerge(hostPortsJson)
//          .deepMerge(memoryUf)
//        _ <- ctx.logger.debug(arsJson.toString)
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
    }

  }

}
