package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{EventXOps, Get, Put, UpdatedNodePort}
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
//
import breeze.linalg._
import breeze.stats.{mean, median}
import breeze.stats.median.reduce_Double
//import breeze.i

object StatsController {


  def apply()(implicit ctx:NodeContext): HttpRoutes[IO] = {

    HttpRoutes.of[IO]{
      case GET -> Root / "stats" =>
        val program = for {
        _                  <- IO.unit
        currentState       <- ctx.state.get
        pendingReplicas    = currentState.pendingReplicas.filter(_._2.rf>0)
        rawEvents          = currentState.events
        nodesQueue         = currentState.nodeQueue
        events             = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        ars                = EventXOps.getAllNodeXs(events=events).map { node =>
              val nodeId = node.nodeId
              val publicPort = Events.getPublicPort(events= events,nodeId).map(_.publicPort).getOrElse(6666)
            node.copy(
              metadata = node.metadata ++ Map("PUBLIC_PORT"->publicPort.toString)
            )
        }
        distributionSchema = Events.generateDistributionSchemaV2(events = events,ctx.config.replicationTechnique)
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
//        _ <- ctx.logger.debug("AFTER_QUEUE 0")
        queueInfo = EventXOps.processQueueTimes(
          events         = events,
          nodeFilterFn   = _=> true,
          mapArrivalTime = e => e match {
            case p:Put => p.arrivalTime
            case p:Get => p.arrivalTime
            case e     => e.monotonicTimestamp
          }
        )
//        _<- ctx.logger.debug("QUEUE 0")
        queueInfoByNode = nodeIds.map(nodeId =>
          nodeId -> EventXOps.processQueueTimes(
            events = events,
            nodeFilterFn = _.nodeId == nodeId,
            mapArrivalTime = e => e match {
              case p:Put => p.arrivalTime
              case p:Get => p.arrivalTime
              case e     => e.monotonicTimestamp
            }

          ),
        ).toMap
//        putsWaitingTimes       = DenseVector.apply(putsQueueTimes.map(_.waitingTime.toDouble).toArray)
//        putsMeanWaitingTime   = mean(putsWaitingTimes)
//        putsMedianWaitingTime = median(putsWaitingTimes)
//
////     ________________________________________________________
//        _ <- IO.unit
//        gets              = EventXOps.onlyGets(events = events)
//        getsATs           = gets.map(_.monotonicTimestamp)
//        getsSTs           = gets.map(_.serviceTimeNanos)
//        getsQueueTimes    = EventXOps.calculateQueueTimes(arrivalTimes = getsATs,serviceTimes = getsSTs)
////      ______________________________________________________________________________________________________________________________________________
//        _ <- IO.unit
//        global            = (puts ++ gets)
//        globalATs         = global.map(_.monotonicTimestamp)
//        globalSTs         = global.map(_.serviceTimeNanos)
//        globalQueueTimes  = EventXOps.calculateQueueTimes(arrivalTimes = globalATs,serviceTimes = globalSTs)
//      ______________________________________________________________________________________________________________________________________________
        pendingPuts        = EventXOps.onlyPendingPuts(events = events)
        pendingGets        = EventXOps.onlyPendingGets(events = events)
        pendingNodes       = (pendingPuts.map(_.nodeId) ++ pendingGets.map(_.nodeId)).distinct

        _ <- IO.unit
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
            "upload" -> currentState.uploadBalancerToken.asJson
          ),
          "apiVersion" -> ctx.config.apiVersion.asJson,
          "queue" -> queueInfo.asJson,
          "queueByNode" -> queueInfoByNode.asJson,
          "pendingReplication" -> pendingReplicas.asJson,
          "pendingNodes"-> pendingNodes.asJson,
          "nodesQueue" -> nodesQueue.asJson

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
