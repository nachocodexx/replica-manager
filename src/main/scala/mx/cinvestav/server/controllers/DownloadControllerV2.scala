package mx.cinvestav.server.controllers

import cats.effect._
import cats.effect.std.Semaphore
import cats.implicits._
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.Get
import mx.cinvestav.events.Events
import org.http4s.dsl.io._
import org.http4s.{AuthedRequest, AuthedRoutes, Header, Headers}
import org.typelevel.ci.CIString
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}

import java.util.UUID
//import
//
import scala.concurrent.duration._
import scala.language.postfixOps

object DownloadControllerV2 {
  def download(operationId:String)(guid:String, authReq:AuthedRequest[IO,User], user:User)(implicit ctx:NodeContext) = for {
//      _ <- ctx.logger.debug(s"INIT_DOWNLOAD $guid")
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      rawEvents        = currentState.events
      events           = Events.orderAndFilterEventsMonotonicV2(rawEvents)
      schema           = Events.generateDistributionSchema(events = events)
      arMap            = Events.getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
      maybeLocations   = schema.get(guid)
      req              = authReq.req
      objectSize       = req.headers.get(CIString("Object-Size")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
      response         <- maybeLocations match {
        case (Some(locations))=> for {
          _                     <- IO.unit
          subsetNodes          = locations.traverse(arMap.get).get.toNel.get
          maybeSelectedNode    = if(subsetNodes.length ==1) subsetNodes.head.some
          else Events.balanceByReplica(currentState.downloadBalancerToken,objectSize = objectSize)(
            guid = guid,
            arMap = subsetNodes.map(x=>x.nodeId->x).toList.toMap,
            events=events,
            monitoringEvents = currentState.monitoringEvents,
            monitoringEx = currentState.monitoringEx
          )
          //            _                    <- ctx.logger.debug(s"SELECTED_NODE ${maybeSelectedNode.map(_.nodeId)}")
          response             <- maybeSelectedNode match {
            case Some(selectedNode) => for {
              _                   <- IO.unit
              serviceTimeNanos    <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
              _                   <- Events.saveEvents(
                events = List(
                  Get(
                    serialNumber=  0,
                    objectId = guid,
                    objectSize= objectSize,
                    timestamp = arrivalTime,
                    nodeId = selectedNode.nodeId,
                    serviceTimeNanos = serviceTimeNanos,
                    userId = user.id.toString,
                    correlationId = operationId
                  )
                )
              )

              serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
//              _ <- ctx.logger.info(s"DOWNLOAD $guid ${selectedNode.nodeId} $serviceTimeNanos")
              nodeUri = if(ctx.config.returnHostname) s"http://${selectedNode.nodeId}:6666/api/v6/download/$guid" else  s"${selectedNode.httpUrl}/api/v6/download/$guid"
//              newResponse <- Ok(s"${selectedNode.httpUrl}/api/v6/download/$guid")
              newResponse <- Ok(nodeUri,Headers(Header.Raw(CIString("Node-Id"),selectedNode.nodeId)))
            } yield newResponse
            case None => InternalServerError()
          }
        } yield response
        case None => for {
          response <- if(ctx.config.cloudEnabled){
            for {
              _                       <- ctx.logger.debug(s"MISS $guid")
              nodes                   = Events.getAllNodeXs(events = events)
              nodesWithAvailablePages = nodes.filter(_.availableCacheSize>0)
              arMap                   = nodes.map(x=>x.nodeId->x).toMap
              maybeSelectedNode            <- if(nodes.length==1) nodes.headOption.pure[IO]
              else if(nodesWithAvailablePages.isEmpty)  for{
                _                  <- ctx.logger.debug("ALL NODES ARE FULL - SELECT A NODE RANDOMLY")
                maybeSelectedNode  = Events.balanceByReplica(downloadBalancer = "PSEUDO_RANDOM")(guid= guid,arMap = arMap,events=events,monitoringEvents=currentState.monitoringEvents)
                //              _                 <- if( (nodes.length < currentState.maxAR) && ctx.config.serviceReplicationDaemon)
//                _ <- if(currentState.serviceReplicationDaemon) Helpers.createNode().start.void else IO.unit
                //              else IO.unit
              } yield maybeSelectedNode
              else nodesWithAvailablePages.maxByOption(_.availableCacheSize).pure[IO]
              response  <- maybeSelectedNode match {
                case Some(selectedNode) => for {
                  _                <- IO.unit
                  selectedNodeId   = selectedNode.nodeId
                  serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)

                  nodeUri = if(ctx.config.returnHostname) s"http://${selectedNode.nodeId}:6666/api/v6/download/$guid" else  s"${selectedNode.httpUrl}/api/v6/download/$guid"
                  newResponse <- Ok(nodeUri,Headers(Header.Raw(CIString("Node-Id"),selectedNode.nodeId)))
                } yield newResponse
                case None => Forbidden()
              }
            } yield response
          }
          else NotFound() <* ctx.errorLogger.debug(s"NOT_FOUND $operationId $guid")
        } yield response
      }
    } yield response

  def apply(sDownload:Semaphore[IO])(implicit ctx:NodeContext) = {

    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "download" / guid as user => for {
        waitingTimeStartAt <- IO.monotonic.map(_.toNanos)
        _                  <- sDownload.acquire
        operationId        = authReq.req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        waitingTimeEndAt   <- IO.monotonic.map(_.toNanos)
        waitingTime        = waitingTimeEndAt - waitingTimeStartAt
        _                  <- ctx.logger.info(s"WAITING_TIME $guid 0 $waitingTime $operationId")
        response           <- download(operationId)(guid.toString,authReq,user)
        headers            = response.headers
        selectedNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("")
        _                  <- sDownload.release
        serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - waitingTimeEndAt)
        _                  <- ctx.logger.info(s"DOWNLOAD $guid $selectedNodeId $serviceTimeNanos $operationId")
        _ <- ctx.logger.debug("____________________________________________________")
      } yield response
    }

  }

}
