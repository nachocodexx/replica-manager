package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.UpdatedNodePort
//
import mx.cinvestav.Declarations.NodeContext
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
      case GET -> Root => for {
        currentState       <- ctx.state.get
        rawEvents          = currentState.events
//        events             = Events.filterEvents(events = currentState.events)
//        events             = Events.orderAndFilterEvents(events = rawEvents)
        events             = Events.orderAndFilterEventsMonotonic(events = rawEvents)
//          Events.filterEvents(events = currentState.events)
        ars                = Events.getAllNodeXs(events=events)
          .map{
            case node =>
              val ops = Events.getOperationsByNodeId(nodeId = node.nodeId,events=rawEvents)
              val gets = ops.count(_.eventType=="GET")
              val puts = ops.count(_.eventType=="PUT")
              node.copy(
                metadata =Map(
                  "TOTAL_REQUESTS" -> (gets+puts).toString,
                  "DOWNLOAD_REQUESTS" -> gets.toString,
                  "UPLOAD_REQUESTS" -> puts.toString,
                )
              )
          }



        distributionSchema = Events.generateDistributionSchema(events = events)
        objectsIds         = Events.getObjectIds(events = events)
        hitCounter         = Events.getHitCounterByNode(events = events).map{
            case (nodeId, counter) =>
              nodeId -> counter.filter(_._2>0)
          }
        hitRatioInfo       = Events.getGlobalHitRatio(events=events)
        tempMatrix         = Events.generateTemperatureMatrixV2(events = events)
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
//                    "diskUF" -> uf.asJson
                      "UF" -> uf.asJson
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
          "nodeId" -> ctx.config.nodeId.asJson,
          "replicationDaemon" -> currentState.replicationDaemon.asJson,
          "replicationDaemonDelay" -> currentState.replicationDaemonDelayMillis.asJson,
          "serviceReplicationDaemon" -> currentState.serviceReplicationDaemon.asJson,
          "serviceReplicationDaemonThreshold" -> currentState.serviceReplicationThreshold.asJson,
          "port"  -> ctx.config.port.asJson,
          "ipAddress" -> currentState.ip.asJson,
          "availableResources" ->arsJson,
//          "ufs" -> ufs,
          "distributionSchema" -> distributionSchema.asJson,
          "objectIds" -> objectsIds.sorted.asJson,
          "nodeIds" -> nodeIds.asJson,
          "hitCounterByNode"-> hitCounter.asJson,
          "tempMatrix" -> tempMatrix.toArray.asJson,
          "hitInfo" -> hitRatioInfo.asJson,
          "loadBalancing" -> Json.obj(
            "download" -> currentState.downloadBalancerToken.asJson,
            "upload" -> currentState.uploadBalancer.map(_.token).getOrElse(ctx.config.uploadLoadBalancer).asJson
          ),
          "maxReplicationFactor" -> currentState.maxRF.asJson,
          "maxAvailableResources" -> currentState.maxAR.asJson,
          "serviceTimes" -> serviceTimeByNode.asJson
        )
        response <- Ok(stats.asJson)
      } yield response
    }

  }

}
