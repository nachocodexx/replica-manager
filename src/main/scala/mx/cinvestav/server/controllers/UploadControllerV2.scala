package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.BalanceResponse
//import mx.cinvestav.con
//
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.balancer.v3.Balancer
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.{NodeX,Monitoring}
import mx.cinvestav.commons.events.EventX
import mx.cinvestav.events.Events
import mx.cinvestav.commons.balancer.{nondeterministic,deterministic}

//
import org.http4s.{AuthedRequest, AuthedRoutes, Header, Headers}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.typelevel.ci.CIString
import org.http4s.circe.CirceEntityEncoder._
//import
//
import java.util.UUID
import concurrent.duration._
import language.postfixOps
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object UploadControllerV2 {


  def commonCode(operationId:String)(
    objectId:String,
    objectSize:Long,
    userId:String,
    events:List[EventX],
    nodes:NonEmptyList[NodeX],
    infos:List[Monitoring.NodeInfo] = List.empty[Monitoring.NodeInfo]
  )(implicit ctx:NodeContext) = {
    for {
//      maybeSelectedNode  <- IO.delay{lb.balance(objectSize = objectSize,nodes)}
      _                  <- IO.unit
      _infos             = NonEmptyList.fromListUnsafe(infos)
      nodeIds             = nodes.map(_.nodeId)
      maybeSelectedNode = ctx.config.uploadLoadBalancer match {
        case "SORTING_UF" => nondeterministic.SortingUF()
          .balance(
            infos = _infos,
            objectSize = objectSize
          ).headOption.flatMap(x=>nodes.find(_.nodeId==x))
        case "TWO_CHOICES" =>
          nondeterministic.TwoChoices(
            psrnd = deterministic.PseudoRandom(
              nodeIds = nodeIds
            )
          ).balance(info = _infos,objectSize = objectSize).some.flatMap(x=>nodes.find(_.nodeId==x))
        case "PSEUDO_RANDOM" =>
          deterministic.PseudoRandom(nodeIds = nodeIds.sorted).balance.some.flatMap(x=>nodes.find(_.nodeId==x))
        case "ROUND_ROBIN" =>
          val defaultCounter = nodeIds.map(x=>x->0).toList.toMap
          val counter        = Events.onlyPutos(events=events).map(_.asInstanceOf[Put]).filterNot(_.replication).groupBy(_.nodeId).map{
              case (nodeId,xs)=>nodeId -> xs.length
            }
          deterministic.RoundRobin(nodeIds = nodeIds)
          .balanceWith(
            nodeIds = nodeIds.sorted,
            counter = counter |+| defaultCounter
          ).some.flatMap(x=>nodes.find(_.nodeId==x))
      }
      response          <- maybeSelectedNode match {
        case Some(node) => for {
          _               <- IO.unit
          selectedNodeId  = node.nodeId
          maybePublicPort = Events.getPublicPort(events,nodeId = node.nodeId).map(x=>( x.publicPort,x.ipAddress))
          res             <- maybePublicPort match {
            case Some((publicPort,ipAddress)) => for{
              _              <- IO.unit
              apiVersionNum  = ctx.config.apiVersion
              apiVersion     = s"v$apiVersionNum"
              usePublicPort  = ctx.config.usePublicPort
              usedPort       = if(!usePublicPort) "6666" else publicPort.toString
              returnHostname = ctx.config.returnHostname
//
              nodeUri    = if(returnHostname) s"http://${node.nodeId}:$usedPort/api/$apiVersion/upload" else  s"http://$ipAddress:$usedPort/api/$apiVersion/upload"
              timestamp  <- IO.realTime.map(_.toMillis)
//            _______________________________________________
              balanceRes = BalanceResponse(
                nodeId       = selectedNodeId,
                dockerPort = 6666,
                publicPort   = publicPort,
                internalIp   = ipAddress,
                timestamp    = timestamp,
                apiVersion   = apiVersionNum,
                dockerURL  = nodeUri,
                operationId  = operationId,
                objectId     = objectId
              )
//            ______________________________________

              resHeaders = Headers(
                  Header.Raw(CIString("Object-Size"),objectSize.toString) ,
                  Header.Raw(CIString("Node-Id"),node.nodeId),
                  Header.Raw(CIString("Public-Port"),publicPort.toString),
              )
              res        <- Ok(balanceRes.asJson,resHeaders)
//              res        <- Ok(nodeUri,
//                Headers(
//                  Header.Raw(CIString("Object-Size"),objectSize.toString) ,
//                  Header.Raw(CIString("Node-Id"),node.nodeId),
//                  Header.Raw(CIString("Public-Port"),publicPort.toString),
//                )
//              )
            } yield res
            case None => ctx.logger.error("NO_PUBLIC_PORT|NO_IP_ADDRESS") *> Forbidden()
          }
        } yield res
        case None =>ctx.logger.error("NO_SELECTED_NODE")*>Forbidden()
      }
    } yield response
  }

  def controller(operationId:String,objectId:String)(
    authReq:AuthedRequest[IO,User],
    user:User,
    rawEvents:List[EventX]=Nil,
    events:List[EventX]=Nil
  )(implicit ctx:NodeContext) =
    for {
//      arrivalTime        <- IO.realTime.map(_.toMillis)
//      arrivalTimeNanos   <- IO.monotonic.map(_.toNanos)
//      currentNodeId      = ctx.config.nodeId
      currentState       <- ctx.state.get
//      rawEvents          = currentState.events
//      events             <- IO.delay{Events.orderAndFilterEventsMonotonicV2(rawEvents)}
//      monitoringInfo     = currentState.mon
      arMap              <- IO.delay{
        ctx.config.uploadLoadBalancer match {
          case "SORTING_UF" | "TWO_CHOICES" => Events.getAllNodeXs(
            events =rawEvents.sortBy(_.monotonicTimestamp)
          ).map(x=> x.nodeId->x).toMap
          case "ROUND_ROBIN" | "PSEUDO_RANDOM" => Events.getAllNodeXs(events=events).map(x=>x.nodeId->x).toMap
          //          case "PSEUDO_RANDOM"  => Events.getAllNodeXs(events=events).map(x=>x.nodeId->x).toMap
        }
      }
      //       NO EMPTY LIST OF RD's
      maybeARNodeX       = NonEmptyList.fromList(arMap.values.toList)
      //   _______________________________________________________________________________
      req                = authReq.req
      headers            = req.headers
      maybeObject        = Events.getObjectById(objectId = objectId,events=events)
      objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
//      maybeLB            = currentState.uploadBalancer
      response <- maybeObject match {
        case Some(o) => for {
          _     <- IO.unit
          res   <- Events.generateDistributionSchema(events = events).get(o.objectId) match {
            case Some(nodes) => for {
              _     <- ctx.logger.debug(s"${o.objectId} ALREADY UPLOADED")
              node = Events.getNodeById(events=events,nodeId = nodes.head).get
              x = if(ctx.config.returnHostname) s"http://${node.nodeId}:6666" else  node.httpUrl
              nodeUri = s"$x/api/v2/upload"

//              balanceResponse = BalanceResponse()

              res <- Ok(nodeUri,Headers(
                Header.Raw(CIString("Already-Uploaded"),"true"),
                Header.Raw(CIString("Node-Id"),node.nodeId),
                Header.Raw(CIString("Object-Size"),objectSize.toString),
                Header.Raw(CIString("Download-Url"),s"$x/api/v6/download/${o.objectId}"),
              ))
            } yield res
            case None => ctx.logger.debug("NO DISTRIBUTION SCHEMA")*> NotFound()
          }
        } yield res
        case None => (maybeARNodeX) match {
//          _________________________________________________
            case Some(nodes) => for {
//              lb  <- Helpers.initLoadBalancerV3(ctx.config.uploadLoadBalancer)
              res <- commonCode(operationId)(objectId = objectId,objectSize = objectSize,userId= user.id,events=events,nodes,infos = currentState.infos)
            } yield res
//          ___________________________________________________
            case Some(nodes) =>
              commonCode(operationId)(objectId = objectId,objectSize = objectSize,userId= user.id,events=events,nodes,infos=currentState.infos)
//          ____________________________________________________
            case None => ctx.logger.debug("NO_NODES,NO_LB") *> Forbidden()
          }
      }
//      departureTime   <- IO.monotonic.map(_.toNanos)
//      serviceTime
    } yield response

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext)={
    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "upload" as user =>
        val defaultConvertion = (x:FiniteDuration) =>  x.toNanos
        val program = for {
//
          serviceTimeStart   <- IO.monotonic.map(defaultConvertion)
//            .map(_ - ctx.initTime)
          //
          now                <- IO.realTime.map(defaultConvertion)
          _                  <- s.acquire
          //      ___________________________________________________________________________________________
          waitingTime        <- IO.monotonic.map(defaultConvertion).map(_ - serviceTimeStart)
//            .map(_ - ctx.initTime)
          //      ______________________________________________________________________________________
          currentState       <- ctx.state.get
          rawEvents          = currentState.events
          events             = Events.orderAndFilterEventsMonotonicV2(rawEvents)
          //      ______________________________________________________________________________________
          headers            = authReq.req.headers
          operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectId           = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
//          arrivalTime        = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
          //      ______________________________________________________________________________________________________________
//          _                  <- ctx.logger.debug(s"TRACE_ARRIVAL_TIME $objectId $arrivalTime")
          _                  <- ctx.logger.debug(s"REAL_ARRIVAL_TIME $objectId $serviceTimeStart")
          //      ______________________________________________________________________________________________________________
          _                  <- ctx.logger.debug(s"SERVICE_TIME_START $objectId $serviceTimeStart")
//        ______________________________________________________________
          response           <- controller(
            operationId=operationId,
            objectId=objectId,
          )(authReq=authReq,user=user, rawEvents=rawEvents, events=events)
//        _____________________________________________________________
          serviceTimeEnd     <- IO.monotonic.map(defaultConvertion).map(_ - ctx.initTime)
//          _                  <- s.release
          serviceTime        = serviceTimeEnd - serviceTimeStart
          //      _________________________________________________________________________________
          headers            = response.headers
          selectedNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("X")
          _                  <- ctx.logger.debug(s"SERVICE_TIME_END $objectId $serviceTimeEnd")
          _                  <- ctx.logger.debug(s"SERVICE_TIME $objectId $serviceTime")
          _                  <- ctx.logger.debug(s"WAITING_TIME $objectId $waitingTime")
          //      ______________________________________________________________________________________
          _                  <- ctx.logger.info(s"UPLOAD $objectId $selectedNodeId $serviceTime $operationId")
          _                  <- Events.saveEvents(
            events =  List(
              Put(
                serialNumber = 0,
                objectId = objectId,
                objectSize = objectSize,
                timestamp = now,
                nodeId = selectedNodeId,
                serviceTimeNanos = serviceTime,
                userId =  user.id,
                serviceTimeEnd = serviceTimeEnd,
                serviceTimeStart = serviceTimeStart,
                waitingTime = waitingTime,
                correlationId = operationId,
                monotonicTimestamp = 0L,
              )
            )
          )
          newResponse        = response.putHeaders(
            Headers(
              Header.Raw(CIString("Waiting-Time"),waitingTime.toString),
              Header.Raw(CIString("Service-Time"),serviceTime.toString),
              Header.Raw(CIString("Service-Time-Start"), serviceTimeStart.toString),
              Header.Raw(CIString("Service-Time-End"), serviceTimeEnd.toString),
            )
          )
          _ <- ctx.logger.debug("____________________________________________________")
          _                  <- s.release

        } yield newResponse
        program.handleErrorWith{e=>
          ctx.logger.debug(e.getMessage)  *> InternalServerError()
      }
    }
  }

}
