package mx.cinvestav.server.controllers

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.Semaphore
import cats.implicits._
import mx.cinvestav.commons.types.BalanceResponse
//
import mx.cinvestav.commons.balancer.{deterministic,nondeterministic}
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.commons.events.{EventX, EventXOps, Get, Put, PutCompleted}
import mx.cinvestav.commons.types.{DumbObject, Monitoring, NodeX}
import mx.cinvestav.events.Events
//
import org.http4s.dsl.io._
import org.http4s.{AuthedRequest, AuthedRoutes, Header, Headers, Response}
import org.http4s.circe.CirceEntityEncoder._
import io.circe.syntax._
import io.circe.generic.auto._
import org.typelevel.ci.CIString
import java.util.UUID
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
//                                infos:List[Monitoring.NodeInfo],
                                waitingTime:Long =0L,
                              )

  //  ____________________________________________________________________
  def processSelectedNode(operationId:String,objectId:String)(events:List[EventX],selectedNode:NodeX)(implicit ctx:NodeContext): IO[Response[IO]] = {
      for {
        _                   <- IO.unit
        selectedNodeId      = selectedNode.nodeId
        maybePublicPort     = Events.getPublicPort(events,nodeId = selectedNodeId)
          .map(x=>(x.publicPort,x.ipAddress))
          .orElse((6666,selectedNodeId).some)

        newResponse         <- maybePublicPort match {
          case Some((publicPort,ipAddress))=> for {
            _               <- IO.unit
            usePublicPort   = ctx.config.usePublicPort
            usedPort        = if(!usePublicPort) "6666" else publicPort.toString
            apiVersionNum   = ctx.config.apiVersion
            apiVersion      = s"v$apiVersionNum"
            nodeUri         = if(ctx.config.returnHostname) s"http://$selectedNodeId:$usedPort/api/$apiVersion/download/$objectId" else  s"http://$ipAddress:$usedPort/api/$apiVersion/download/$objectId"
//            returnHostname  = ctx.config.returnHostname
            timestamp       <- IO.realTime.map(_.toMillis)
            balanceResponse = BalanceResponse(
              nodeId       = selectedNodeId,
              dockerPort  = 6666,
              publicPort  = publicPort,
              internalIp  = ipAddress,
              timestamp   = timestamp,
              apiVersion  = apiVersionNum,
              dockerURL   = nodeUri,
              operationId = operationId,
              objectId    = objectId,
              ufs         = selectedNode.ufs
            )
            res             <- Ok(
              balanceResponse.asJson,
              Headers(
                Header.Raw(CIString("Node-Id"),selectedNodeId),
                Header.Raw(CIString("Public-Port"),publicPort.toString),
              )
            )
          } yield res
          case None =>
            ctx.logger.error("NO_PUBLIC_PORT | NO_IP_ADDRESS") *> Forbidden()
        }
      } yield newResponse
  }
  //  ____________________________________________________________________

  def balance(locations:List[String],arMap:Map[String,NodeX],objectSize:Long,gets:List[EventX])(implicit ctx:NodeContext) = {
    val locationNodes           = locations.map(arMap)
    val filteredNodes           = locationNodes.filter(_.availableMemoryCapacity >= objectSize)
    val filteredLocationUfs     = filteredNodes.map(_.ufs)
    //      _____________________________________________________________________________________________________________________________________
     if(filteredNodes.isEmpty) None
    else if(filteredNodes.length == 1) filteredNodes.head.some
    else {
      val filteredLocationNodeIds = filteredNodes.map(_.nodeId)
      val _locationNodeIds        = NonEmptyList.fromListUnsafe(filteredLocationNodeIds)
      ctx.config.downloadLoadBalancer match {
        case "ROUND_ROBIN"   =>
          val counter          = gets.groupBy(_.nodeId).map(x=> x._1-> x._2.length).filter(x=> _locationNodeIds.contains_(x._1))
          val selectedNodeId   = deterministic.RoundRobin(nodeIds = _locationNodeIds).balanceWith(nodeIds = _locationNodeIds,counter = counter)
          arMap.get(selectedNodeId)
        case "PSEUDO_RANDOM" =>
          val selectedNodeId = deterministic.PseudoRandom(nodeIds = _locationNodeIds).balance
          arMap.get(selectedNodeId)
        case "TWO_CHOICES"   =>
          val psrnd               = deterministic.PseudoRandom(nodeIds = _locationNodeIds)
          val maybeSelectedNodeId = nondeterministic.TwoChoices(psrnd = psrnd)
            .balances(ufs =  filteredLocationUfs,mapUf = _.memoryUF)
            .headOption
          maybeSelectedNodeId.flatMap(arMap.get)
        case "SORTING_UF" =>
          val maybeSelectedNodeId = nondeterministic.SortingUF()
            .balance(ufs = filteredLocationUfs,mapUf = _.memoryUF)
            .headOption
          maybeSelectedNodeId.flatMap(arMap.get)
      }
    }
  }
  def success(operationId:String,locations:List[String])(x: PreDownloadParams)(implicit ctx:NodeContext) = {
    val program = for {
      _                     <- IO.unit
      //    __________________________________________________
      events                  = x.events
      gets                    = EventXOps.onlyGetCompleteds(events = events)
      userId                  = x.userId
      objectId                = x.objectId
      objectSize              = x.objectSize
      arMap                   = x.arMap
//      subsetNodes             = locations.traverse(arMap.get).get.toNel.get
      _                       <- ctx.logger.debug(s"LOCATIONS $objectId $locations")
      maybeSelectedNode       = balance(
        locations  = locations,
        arMap      = arMap,
        objectSize = objectSize,
        gets       = gets
      )
//      filteredNodes           = locationNodes.filter(_.availableMemoryCapacity >= objectSize)
//      filteredLocationUfs     = filteredNodes.map(_.ufs)
//      //      _____________________________________________________________________________________________________________________________________
//      maybeSelectedNode     = if(filteredNodes.isEmpty) None
//      else if(filteredNodes.length == 1) subsetNodes.head.some
//      else {
//        val filteredLocationNodeIds = filteredNodes.map(_.nodeId)
//        val _locationNodeIds        = NonEmptyList.fromListUnsafe(filteredLocationNodeIds)
//        ctx.config.downloadLoadBalancer match {
//          case "ROUND_ROBIN"   =>
////            val counter          = gets.groupBy(_.nodeId).map(x=> x._1-> x._2.length).filter(x=> _locationNodeIds.contains_(x._1))
//            val counter                 = gets.groupBy(_.nodeId).map(x=> x._1-> x._2.length).filter(x=> _locationNodeIds.contains_(x._1))
//            val selectedNodeId   = deterministic.RoundRobin(nodeIds = _locationNodeIds).balanceWith(nodeIds = _locationNodeIds,counter = counter)
//            arMap.get(selectedNodeId)
//          case "PSEUDO_RANDOM" =>
//            val selectedNodeId = deterministic.PseudoRandom(nodeIds = _locationNodeIds).balance
//            arMap.get(selectedNodeId)
//          case "TWO_CHOICES"   =>
//            val psrnd               = deterministic.PseudoRandom(nodeIds = _locationNodeIds)
//            val maybeSelectedNodeId = nondeterministic.TwoChoices(psrnd = psrnd)
//              .balances(ufs =  filteredLocationUfs,mapUf = _.memoryUF)
//              .headOption
//            maybeSelectedNodeId.flatMap(arMap.get)
//          case "SORTING_UF" =>
//            val maybeSelectedNodeId = nondeterministic.SortingUF()
//              .balance(ufs = filteredLocationUfs,mapUf = _.memoryUF)
//              .headOption
//            maybeSelectedNodeId.flatMap(arMap.get)
//        }
//      }
      //      _____________________________________________________________________________________________________________________________________
      _                     <- ctx.logger.debug(s"SELECTED_NODE $maybeSelectedNode")
      response              <- maybeSelectedNode match {
        case Some(selectedNode) => for {
          _                  <- IO.unit
          newResponse        <- processSelectedNode(operationId = operationId,objectId = objectId)(events, selectedNode)
        } yield newResponse
        case None => for {
          currentState  <- ctx.state.get
          misses        = currentState.misses
          missesCounter = misses.getOrElse(objectId,1)
          _             <- ctx.logger.debug(s"MISS $objectId $missesCounter")
          _             <- ctx.state.update(s=> s.copy(misses = s.misses.updatedWith(objectId){ op =>
              op match {
                case Some(value) => (value+1).some
                case None => 1.some
              }
            }

          ))
          res          <- Accepted()
        } yield res
//          ctx.logger.error("NO_SELECTED_NODE_SUCCESS") *> Forbidden()
      }
    } yield response

    program.handleErrorWith{ e =>
      ctx.logger.debug(e.getMessage) *> InternalServerError()
    }
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
//      maxAR                    = x.maxAR
//      serviceReplicationDaemon = x.serviceReplicationDaemon
//    ___________________________________________________________
      predicate             = ctx.config.cloudEnabled  || ctx.config.hasNextPool
      response              <- if(predicate){
        for {
          _                 <- ctx.logger.debug(s"MISS $objectId")
          nodes             = EventXOps.getAllNodeXs(events = events)
          availableNodes    = nodes.filter(_.availableMemoryCapacity>= objectSize)
          arMap             = nodes.map(x=>x.nodeId->x).toMap
          maybeSelectedNode = Option.empty[NodeX]
//          maybeSelectedNode <- if(nodes.length==1) nodes.headOption.pure[IO]
//          else if(availableNodes.isEmpty)  for{
//            _   <- ctx.logger.debug("ALL NODES ARE FULL - SELECT A NODE RANDOMLY")
//            res = None
//          } yield res
//          else availableNodes.maxByOption(_.availableCacheSize).pure[IO]
          //        _________________________________________________________________________
          response <- maybeSelectedNode match {
            case Some(selectedNode) => processSelectedNode(operationId = operationId,objectId = objectId)(events, selectedNode)
            case None => Accepted()
          }
        } yield response
      }
      else NotFound() <* ctx.errorLogger.debug(s"NOT_FOUND $operationId $objectId")
    } yield response
  }
  //  ____________________________________________________________________
  def download(operationId:String)(dumbObject: DumbObject, authReq:AuthedRequest[IO,User], user:User)(implicit ctx:NodeContext) = for {
    arrivalTime          <- IO.realTime.map(_.toMillis)
    arrivalTimeNanos     <- IO.monotonic.map(_.toNanos)
    currentState         <- ctx.state.get
    req                  = authReq.req
    objectId             = dumbObject.objectId
    objectSize           = dumbObject.objectSize
    rawEvents            = currentState.events
    events               = Events.orderAndFilterEventsMonotonicV2(rawEvents)
    maybePendingDownload = EventXOps.pendingGetByOperationId(events = events, operationId = operationId)
    downloadProgram      = for{
        _                    <- IO.unit
        schema               = Events.generateDistributionSchemaV2(events = events,ctx.config.replicationMethod)
        arMap                = EventXOps.getAllNodeXs(events = events,objectSize = objectSize).map(x=>x.nodeId->x).toMap
        maybeLocations       = schema.get(objectId)
        preDownloadParams    = PreDownloadParams(
          events                   = events,
          arrivalTimeNanos         = arrivalTimeNanos,
          arrivalTime              = arrivalTime,
          userId                   = user.id,
          downloadBalancerToken    = currentState.downloadBalancerToken,
          objectId                 = objectId,
          objectSize               = objectSize,
          arMap                    = arMap,
//          infos                    = currentState.infos,
        )
        //    _____________________________________________________________________________________________________________________
        response             <- maybeLocations match {
          case (Some(locations)) => success(operationId,locations)(preDownloadParams)
          case None              => notFound(operationId)(preDownloadParams)
          //            NotFound()
        }
      } yield response

    response             <- maybePendingDownload match {
      case Some(value) => ctx.logger.debug(s"GET_PENDING $operationId") *> downloadProgram
      case None => downloadProgram
    }

    } yield response

  def downloads(operationId:String)(authReq: AuthedRequest[IO,User],user:User)(implicit ctx:NodeContext) = for {
    arrivalTime       <- IO.realTime.map(_.toMillis)
    arrivalTimeNanos  <- IO.monotonic.map(_.toNanos)
    currentState      <- ctx.state.get
    rawEvents         = currentState.events
    events            = Events.orderAndFilterEventsMonotonicV2(rawEvents)
    schema            = Events.generateDistributionSchemaV2(events = events,ctx.config.replicationMethod)
    arMap             = EventXOps.getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
    objects           = EventXOps
      .onlyPutCompleteds(events = events)
      .map(_.asInstanceOf[PutCompleted])
      .filter(_.correlationId == operationId)
      .map(x=> DumbObject(objectId = x.objectId, objectSize = x.objectSize ))
    req               = authReq.req
  } yield ()


  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = {

    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "downloads" / operationId as user => for {
        serviceTimeStart   <- IO.monotonic.map(_.toNanos)
        now                <- IO.realTime.map(_.toNanos)
        _                  <- s.acquire
        headers            = authReq.req.headers
        operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        //
        waitingTime        <- IO.monotonic.map(_.toNanos).map(_ - serviceTimeStart)
        //      ________________________________________________________________
        response          <- Ok()
        //      ________________________________________________________________
        headers            = response.headers
        selectedNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("")
        //      ________________________________________________________________
        serviceTimeEnd     <- IO.monotonic.map(_.toNanos)
        serviceTime        = serviceTimeEnd - serviceTimeStart
//        get                = Get(
//          serialNumber     =  0,
//          objectId         = operationId,
//          objectSize       = objectSize,
//          timestamp        = now,
//          nodeId           = selectedNodeId,
//          serviceTimeNanos = serviceTime,
//          userId           = user.id,
//          correlationId    = operationId,
//          serviceTimeEnd   = serviceTimeEnd,
//          serviceTimeStart = serviceTimeStart,
////          waitingTime      = waitingTime
//        )
//        _                  <- Events.saveEvents(events = get::Nil)
        _                  <- ctx.logger.info(s"DOWNLOAD $operationId $selectedNodeId $serviceTime $operationId")
        newResponse        = response.putHeaders(
          Headers(
            Header.Raw(CIString("Waiting-Time"),waitingTime.toString),
            Header.Raw(CIString("Service-Time"),serviceTime.toString),
            Header.Raw(CIString("Service-Time-Start"),serviceTimeStart.toString),
            Header.Raw(CIString("Service-Time-End"),serviceTimeEnd.toString),
          )
        )
        _ <- ctx.logger.debug("____________________________________________________")
        _                  <- s.release
      } yield newResponse
//    }
      case authReq@GET -> Root / "download" / objectId as user =>
        val monotonic = IO.monotonic.map(_.toNanos)
        val realTime  = IO.realTime.map(_.toNanos)
        for {
          _                    <- s.acquire
          serviceTimeStart     <- monotonic
          currrentTimestamp    <- realTime
          headers              = authReq.req.headers
          operationId          = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectSize           = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
          requestStartAt       = headers.get(CIString("Request-Start-At")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
          latency              = serviceTimeStart - requestStartAt
          dumbObject           = DumbObject(objectId = objectId, objectSize = objectSize)
          //      _________________________________________________________________________
          waitingTime          <- monotonic.map(_ - serviceTimeStart)
          //      ________________________________________________________________________
          response             <- download(operationId)(dumbObject = dumbObject,authReq,user)
          //      _______________________________________________________________________
          headers              = response.headers
          selectedNodeId       = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("")
          //      _______________________________________________________________________
          serviceTimeEnd       <- monotonic
          serviceTime          = serviceTimeEnd - serviceTimeStart
          _                    <- if(selectedNodeId.nonEmpty) {
            val get                  = Get(
              serialNumber     =  0,
              objectId         = objectId,
              objectSize       = objectSize,
              timestamp        = currrentTimestamp,
              nodeId           = selectedNodeId,
              serviceTimeNanos = serviceTime,
              userId           = user.id,
              correlationId    = operationId,
              serviceTimeEnd   = serviceTimeEnd,
              serviceTimeStart = serviceTimeStart,
              //          waitingTime      = waitingTime
            )
             Events.saveEvents(events = get::Nil) *> ctx.logger.info(s"DOWNLOAD $objectId $selectedNodeId $serviceTime $operationId")
          } else ctx.logger.debug(s"NO_AVAILABLE_NODE $operationId $objectId")
          newResponse          = response.putHeaders(
          Headers(
            Header.Raw(CIString("Latency"),latency.toString),
            Header.Raw(CIString("Waiting-Time"),waitingTime.toString),
            Header.Raw(CIString("Service-Time"),serviceTime.toString),
            Header.Raw(CIString("Service-Time-Start"),serviceTimeStart.toString),
            Header.Raw(CIString("Service-Time-End"),serviceTimeEnd.toString),
          )
        )
          _                    <- ctx.logger.debug("____________________________________________________")
          _                    <- s.release
      } yield newResponse
    }

  }

}
