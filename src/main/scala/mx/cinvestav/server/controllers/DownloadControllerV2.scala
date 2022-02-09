package mx.cinvestav.server.controllers

import cats.effect._
import cats.effect.std.Semaphore
import cats.implicits._
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{EventX, Get}
import mx.cinvestav.commons.types.{Monitoring, NodeX}
import mx.cinvestav.events.Events
import org.http4s.dsl.io._
import org.http4s.{AuthedRequest, AuthedRoutes, Header, Headers, Response}
import org.typelevel.ci.CIString
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}

import java.util.UUID
//import
//
import scala.concurrent.duration._
import scala.language.postfixOps

object DownloadControllerV2 {
  case class PreDownloadParams(
                                events:List[EventX],
                                arrivalTimeNanos:Long,
                                arrivalTime:Long,
                                userId:String,
                                downloadBalancerToken:String,
                                objectId:String,
                                objectSize:Long,
                                arMap:Map[String,NodeX],
                                infos:List[Monitoring.NodeInfo],
                                maxAR:Int,
                                serviceReplicationDaemon:Boolean
                              )

  //  ____________________________________________________________________
  def processSelectedNode(objectId:String)(events:List[EventX],maybeSelectedNode:Option[NodeX])(implicit ctx:NodeContext): IO[Response[IO]] = {
     maybeSelectedNode match {
      case Some(selectedNode) => for {
        _                   <- IO.unit
        selectedNodeId      = selectedNode.nodeId
        maybePublicPort     = Events.getPublicPort(events,nodeId = selectedNodeId).map(x=>(x.publicPort,x.ipAddress))
        newResponse         <- maybePublicPort match {
          case Some((publicPort,ipAddress))=> for {
            _                <- IO.unit
            apiVersion       = s"v${ctx.config.apiVersion}"
            usedPort         = if(ctx.config.usePublicPort) publicPort else "6666"
//            ipAddress        = selectedNode.ip
            nodeUri          = if(ctx.config.returnHostname) s"http://$selectedNodeId:$usedPort/api/$apiVersion/download/$objectId" else  s"http://$ipAddress:$usedPort/api/$apiVersion/download/$objectId"
            res              <- Ok(
              nodeUri,
              Headers(
                Header.Raw(CIString("Node-Id"),selectedNode.nodeId),
                Header.Raw(CIString("Public-Port"),publicPort.toString),
              )
            )
          } yield res
          case None => Forbidden()
        }
      } yield newResponse
      case None => Forbidden()
    }
  }
  //  ____________________________________________________________________
  def success(operationId:String,locations:List[String])(x: PreDownloadParams)(implicit ctx:NodeContext) = {
    for {
      _                     <- IO.unit
//    __________________________________________________
      events                = x.events
      arrivalTimeNanos      = x.arrivalTimeNanos
      arrivalTime           = x.arrivalTime
      userId                = x.userId
      downloadBalancerToken = x.downloadBalancerToken
      objectId              = x.objectId
      objectSize            = x.objectSize
      arMap                 = x.arMap
      infos                 = x.infos
      subsetNodes           = locations.traverse(arMap.get).get.toNel.get
//    ___________________________________________________
      maybeSelectedNode     = if(subsetNodes.length ==1) subsetNodes.head.some else Events.balanceByReplica(
        downloadBalancerToken,
        objectSize = objectSize)(
        guid = objectId,
        arMap = subsetNodes.map(x=>x.nodeId->x).toList.toMap,
        events=events,
        monitoringEx = infos
      )
      response              <- maybeSelectedNode match {
        case Some(selectedNode) => for {
          _                  <- IO.unit
          serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
          selectedNodeId     = selectedNode.nodeId
          get                = Get(
                serialNumber=  0,
                objectId = objectId,
                objectSize= objectSize,
                timestamp = arrivalTime,
                nodeId = selectedNodeId,
                serviceTimeNanos = serviceTimeNanos,
                userId = userId,
                correlationId = operationId
          )
          newResponse        <- processSelectedNode(objectId)(events, maybeSelectedNode)
          _                  <- Events.saveEvents(events = get::Nil)
        } yield newResponse
        case None => Forbidden()
      }
    } yield response
  }
  //  ____________________________________________________________________
  def notFound(operationId:String)(x: PreDownloadParams)(implicit ctx:NodeContext) = {
    for {
      _                     <- IO.unit
      events                   = x.events
      arrivalTimeNanos         = x.arrivalTimeNanos
      arrivalTime              = x.arrivalTime
      userId                   = x.userId
      downloadBalancerToken    = x.downloadBalancerToken
      objectId                 = x.objectId
      objectSize               = x.objectSize
      arMap                    = x.arMap
      infos                    = x.infos
      maxAR                    = x.maxAR
      serviceReplicationDaemon = x.serviceReplicationDaemon
//    ___________________________________________________________
      response              <- if(ctx.config.cloudEnabled){
        for {
          _                       <- ctx.logger.debug(s"MISS $objectId")
          nodes                   = Events.getAllNodeXs(events = events)
          nodesWithAvailablePages = nodes.filter(_.availableCacheSize>0)
          arMap                   = nodes.map(x=>x.nodeId->x).toMap
          maybeSelectedNode       <- if(nodes.length==1) nodes.headOption.pure[IO]
          else if(nodesWithAvailablePages.isEmpty)  for{
            _                  <- ctx.logger.debug("ALL NODES ARE FULL - SELECT A NODE RANDOMLY")
            maybeSelectedNode  = Events.balanceByReplica(downloadBalancer = "PSEUDO_RANDOM")(guid= objectId,arMap = arMap,events=events, monitoringEx = infos)
          } yield maybeSelectedNode
          else nodesWithAvailablePages.maxByOption(_.availableCacheSize).pure[IO]
//        _________________________________________________________________________
          response <- processSelectedNode(objectId)(events, maybeSelectedNode)
        } yield response
      }
      else NotFound() <* ctx.errorLogger.debug(s"NOT_FOUND $operationId $objectId")
    } yield response
  }
  //  ____________________________________________________________________
  def download(operationId:String)(objectId:String, authReq:AuthedRequest[IO,User], user:User)(implicit ctx:NodeContext) = for {
    arrivalTime       <- IO.realTime.map(_.toMillis)
    arrivalTimeNanos  <- IO.monotonic.map(_.toNanos)
    currentState      <- ctx.state.get
    rawEvents         = currentState.events
    events            = Events.orderAndFilterEventsMonotonicV2(rawEvents)
    schema            = Events.generateDistributionSchema(events = events)
    arMap             = Events.getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
    maybeLocations    = schema.get(objectId)
    req               = authReq.req
    objectSize        = req.headers.get(CIString("Object-Size")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
    preDownloadParams = PreDownloadParams(
        events                   = events,
        arrivalTimeNanos         = arrivalTimeNanos,
        arrivalTime              = arrivalTime,
        userId                   = user.id,
        downloadBalancerToken    = currentState.downloadBalancerToken,
        objectId                 = objectId,
        objectSize               = objectSize,
        arMap                    = arMap,
        infos                    = currentState.infos,
        maxAR                    = currentState.maxAR,
        serviceReplicationDaemon = currentState.serviceReplicationDaemon
      )
    //    _____________________________________________________________________________________________________________________
    response         <- maybeLocations match {
        case (Some(locations)) => success(operationId,locations)(preDownloadParams)
        case None              => notFound(operationId)(preDownloadParams)
    }
    } yield response

  def apply(sDownload:Semaphore[IO])(implicit ctx:NodeContext) = {

    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "download" / objectId as user => for {
        waitingTimeStartAt <- IO.monotonic.map(_.toNanos)
        _                  <- sDownload.acquire
        operationId        = authReq.req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        waitingTimeEndAt   <- IO.monotonic.map(_.toNanos)
        waitingTime        = waitingTimeEndAt - waitingTimeStartAt
//      ________________________________________________________________
        response           <- download(operationId)(objectId,authReq,user)
//      ________________________________________________________________
        headers            = response.headers
        selectedNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("")
//      ________________________________________________________________
        _                  <- sDownload.release
        serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - waitingTimeEndAt)
        _                  <- ctx.logger.info(s"DOWNLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
        newResponse        = response.putHeaders(
          Headers(
            Header.Raw(CIString("Waiting-Time"),waitingTime.toString),
            Header.Raw(CIString("Service-Time"),serviceTimeNanos.toString)
          )
        )
        _ <- ctx.logger.debug("____________________________________________________")
      } yield newResponse
    }

  }

}
