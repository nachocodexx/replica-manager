package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.BalanceResponse
import mx.cinvestav.commons.events.{EventXOps, PutCompleted}
import mx.cinvestav.commons.types.DumbObject
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


  def alreadyUploaded(o:DumbObject,events:List[EventX])(implicit ctx:NodeContext) = {
    for {
      _     <- IO.unit
      res   <- Events.generateDistributionSchema(events = events).get(o.objectId) match {
        case Some(nodes) => for {
          _     <- ctx.logger.debug(s"${o.objectId} ALREADY UPLOADED")
          node = Events.getNodeById(events=events,nodeId = nodes.head).get
          x = if(ctx.config.returnHostname) s"http://${node.nodeId}:6666" else  node.httpUrl
          nodeUri = s"$x/api/v2/upload"

          res <- Ok(nodeUri,Headers(
            Header.Raw(CIString("Already-Uploaded"),"true"),
            Header.Raw(CIString("Node-Id"),node.nodeId),
            Header.Raw(CIString("Object-Size"),o.objectSize.toString),
            Header.Raw(CIString("Download-Url"),s"$x/api/v6/download/${o.objectId}"),
          ))
        } yield res
        case None => ctx.logger.debug("NO DISTRIBUTION SCHEMA")*> NotFound()
      }
    } yield res
  }

  def commonCode(operationId:String)(
    objectId:String,
    objectSize:Long,
    userId:String,
    events:List[EventX],
    nodes:NonEmptyList[NodeX],
  )(implicit ctx:NodeContext) = {
    for {
      _                  <- IO.unit
      nodeIds             = nodes.map(_.nodeId)
      ufs                 = nodes.map(_.ufs).toList
      maybeSelectedNode = ctx.config.uploadLoadBalancer match {
        case "SORTING_UF" =>
          val selectedNodeIds = nondeterministic.SortingUF()
          .balance(ufs = ufs)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId) ).sequence
          xs
        case "TWO_CHOICES" =>
          val selectedNodeIds = nondeterministic.TwoChoices(
            psrnd = deterministic.PseudoRandom(nodeIds = nodeIds)
          ).balances(ufs =ufs)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId)).sequence
          xs
        case "PSEUDO_RANDOM" =>
          val selectedNodeIds = deterministic.PseudoRandom(nodeIds = nodeIds.sorted)
            .balanceReplicas(replicaNodes = Nil,takeN=1)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId)).sequence
          xs
        case "ROUND_ROBIN" =>
          val defaultCounter = nodeIds.map(x=>x->0).toList.toMap
          val lb      = deterministic.RoundRobin(nodeIds = nodeIds)
          val counter = Events.onlyPutos(events=events).map(_.asInstanceOf[Put]).filterNot(_.replication).groupBy(_.nodeId).map{
              case (nodeId,xs)=>nodeId -> xs.length
            }
          val pivotNode = lb.balanceWith(nodeIds =nodeIds,counter = counter |+| defaultCounter)
          val selectedNodes  = lb.balanceReplicas(
            replicaNodes = pivotNode::Nil,
            takeN =  1
          )
          val selectedNodes0 = (List(pivotNode) ++ selectedNodes)
          val xs = selectedNodes0.map(nodeId => nodes.find(_.nodeId == nodeId)).sequence
          xs
      }
      response          <- maybeSelectedNode match {
        case Some(nodes) => for {
          _        <- IO.unit
          xs       <- nodes.traverse{ node =>
            for {
              _               <- IO.unit
              selectedNodeId  = node.nodeId
              maybePublicPort = Events.getPublicPort(events,nodeId = selectedNodeId).map(x=>( x.publicPort,x.ipAddress))
              res             <- maybePublicPort match {
                case Some((publicPort,ipAddress)) => for{
                  _              <- IO.unit
                  apiVersionNum  = ctx.config.apiVersion
                  apiVersion     = s"v$apiVersionNum"
                  usePublicPort  = ctx.config.usePublicPort
                  usedPort       = if(!usePublicPort) "6666" else publicPort.toString
                  returnHostname = ctx.config.returnHostname
                  //
                  nodeUri    = if(returnHostname) s"http://${selectedNodeId}:$usedPort/api/$apiVersion/upload" else  s"http://$ipAddress:$usedPort/api/$apiVersion/upload"
                  timestamp  <- IO.realTime.map(_.toMillis)
                  //            _______________________________________________
                  balanceRes = BalanceResponse(
                    nodeId       = selectedNodeId,
                    dockerPort  = 6666,
                    publicPort  = publicPort,
                    internalIp  = ipAddress,
                    timestamp   = timestamp,
                    apiVersion  = apiVersionNum,
                    dockerURL   = nodeUri,
                    operationId = operationId,
                    objectId    = objectId,
                    ufs         = node.ufs
                  )
                  //            ______________________________________
                  resHeaders = Headers(
                    Header.Raw(CIString("Object-Size"),objectSize.toString) ,
                    Header.Raw(CIString("Node-Id"),selectedNodeId),
                    Header.Raw(CIString("Public-Port"),publicPort.toString),
                  )
                  //              res        <- Ok(balanceRes:.asJson,resHeaders)
//                  res <- Ok()
                } yield (balanceRes,resHeaders)
                case None => ctx.logger.error("NO_PUBLIC_PORT|NO_IP_ADDRESS") *> (BalanceResponse.empty,Headers.empty).pure[IO]
              }
//            _______________________________________________________________
            } yield res
          }
          headers  = xs.map(_._2).foldLeft(Headers.empty)(_ ++ _)
          balances = xs.map(_._1)
          res      <- Ok(balances.asJson,headers)
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
      currentState       <- ctx.state.get
      req                = authReq.req
      headers            = req.headers
      maybeObject        = Events.getObjectByIdV2(objectId = objectId,events=events)
      objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      arMap              = ctx.config.uploadLoadBalancer match {
        case "SORTING_UF" | "TWO_CHOICES" => EventXOps.getAllNodeXs(
          events     = rawEvents.sortBy(_.monotonicTimestamp),
          objectSize = objectSize
        ).map(x=> x.nodeId->x).toMap
        case "ROUND_ROBIN" | "PSEUDO_RANDOM" => EventXOps.getAllNodeXs(
          events=events,
          objectSize = objectSize
        ).map(x=>x.nodeId->x).toMap
      }
      //       NO EMPTY LIST OF RD's
      maybeARNodeX       = NonEmptyList.fromList(arMap.values.toList)
      //   _______________________________________________________________________________
      response           <- maybeObject match {
        case Some(o) => alreadyUploaded(o,events=events)
        case None => maybeARNodeX match {
//          _________________________________________________
            case Some(nodes) => commonCode(operationId)(
              objectId = objectId,
              objectSize = objectSize,
              userId= user.id,
              events=events,
              nodes,

            )
//          ____________________________________________________
            case None => ctx.logger.debug("NO_NODES,NO_LB") *> Forbidden()
          }
      }
    } yield response

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext)={
    AuthedRoutes.of[User,IO]{
//    __________________________________________________________________________________
      case authReq@POST -> Root / "upload" / operationId / objectId as user =>
        val program = for {
          currentState <- ctx.state.get
  //      ________________________________________________________________________
          events       = Events.filterEventsMonotonicV2(events = currentState.events)
          puts         = Events.onlyPutos(events = events).map(_.asInstanceOf[Put])
          maybePut     = puts.find(p => p.correlationId == operationId && p.objectId == objectId)
  //      ________________________________________________________________________
          res          <- maybePut match {
            case Some(put) => for {
              res          <- NoContent()
              completedPut = PutCompleted.fromPut(p = put)
              _            <- Events.saveEvents(events = completedPut::Nil)
            } yield res
//          ________________________________________________________________________
            case None => NotFound()
          }
        } yield res
        program.handleErrorWith{ e =>
          ctx.logger.error(e.getMessage) *> InternalServerError()
        }
//    __________________________________________________________________________________
      case authReq@POST -> Root / "upload" as user =>
        val defaultConvertion = (x:FiniteDuration) =>  x.toNanos
        val monotonic         = IO.monotonic.map(defaultConvertion)
        val program = for {
          serviceTimeStart   <- monotonic
          now                <- IO.realTime.map(defaultConvertion)
//      ______________________________________________________________________________________
          currentState       <- ctx.state.get
          rawEvents          = currentState.events
          events             = Events.orderAndFilterEventsMonotonicV2(rawEvents)
//      ______________________________________________________________________________________
          headers            = authReq.req.headers
          operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectId           = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
//      ______________________________________________________________________________________________________________
          _                  <- ctx.logger.debug(s"ARRIVAL_TIME $objectId $serviceTimeStart")
//      ______________________________________________________________________________________________________________
          _                  <- ctx.logger.debug(s"SERVICE_TIME_START $objectId $serviceTimeStart")
//        ___________________________________________________________________________________
          response           <- controller(
            operationId=operationId,
            objectId=objectId,
          )(authReq=authReq,user=user, rawEvents=rawEvents, events=events)
//        _____________________________________________________________
          serviceTimeEnd     <- monotonic
          serviceTime        = serviceTimeEnd - serviceTimeStart
//       _______________________________________________________________________________
          headers            = response.headers
          selectedNodeIds    = headers.get(CIString("Node-Id")).map(_.toList.map(_.value)).getOrElse(List.empty[String])
          _                  <- ctx.logger.debug(s"SERVICE_TIME_END $objectId $serviceTimeEnd")
          _                  <- ctx.logger.debug(s"SERVICE_TIME $objectId $serviceTime")
//      ______________________________________________________________________________________
          _events            = selectedNodeIds.map{ selectedNodeId =>
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
              correlationId = operationId,
              monotonicTimestamp = 0L,
            )
          }
          _                  <- Events.saveEvents(_events)
//      ______________________________________________________________________________________
          newResponse        = response.putHeaders(
            Headers(
              Header.Raw(CIString("Service-Time"),serviceTime.toString),
              Header.Raw(CIString("Service-Time-Start"), serviceTimeStart.toString),
              Header.Raw(CIString("Service-Time-End"), serviceTimeEnd.toString),
            )
          )
          _                  <- ctx.logger.debug("____________________________________________________")
        } yield newResponse
//      ______________________________________________________________________________________
        program.handleErrorWith{e=>
          ctx.logger.debug(e.getMessage)  *> InternalServerError()
      }
    }
  }

}
