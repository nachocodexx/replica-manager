package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import org.http4s.Response
//import mx.cinvestav.Declarations.BalanceResponse
import mx.cinvestav.commons.events.{EventXOps, PutCompleted}
import mx.cinvestav.commons.types.DumbObject
//import mx.cinvestav.con
//
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.balancer.v3.Balancer
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.{NodeX,Monitoring,BalanceResponse}
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


  def alreadyUploaded(o:DumbObject,events:List[EventX])(implicit ctx:NodeContext): IO[Response[IO]] = {

    for {
      _    <- ctx.logger.debug(s"PUT_PENDING ${o.objectId}")

      res  <- Accepted()
    } yield res

  }

  def commonCode(operationId:String)(
    objectId:String,
    objectSize:Long,
    userId:String,
    events:List[EventX],
    nodes:NonEmptyList[NodeX],
    rf:Int = 1
  )(implicit ctx:NodeContext) = {
    for {
      _                  <- IO.unit
      nodeIds            = nodes.map(_.nodeId)
//      ufs                = nodes.map(_.ufs).toList
      filteredNodes      = nodes.filter(_.availableStorageCapacity >= objectSize)
      filteredNodeIds    = filteredNodes.map(_.nodeId)
      filteredUfs        = filteredNodes.map(_.ufs)

      maybeSelectedNode =if(filteredNodes.isEmpty) Option.empty[List[NodeX]]  else ctx.config.uploadLoadBalancer match {
        case "SORTING_UF" =>
          val selectedNodeIds = nondeterministic.SortingUF()
          .balance(ufs = filteredUfs,takeN = rf)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId) ).sequence
          xs
        case "TWO_CHOICES" =>
          val selectedNodeIds = nondeterministic.TwoChoices(
            psrnd = deterministic.PseudoRandom(nodeIds = NonEmptyList.fromListUnsafe(filteredNodeIds))
          ).balances(ufs =filteredUfs,takeN = rf)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId)).sequence
          xs
        case "PSEUDO_RANDOM" =>
          val selectedNodeIds = deterministic.PseudoRandom(nodeIds = NonEmptyList.fromListUnsafe(filteredNodeIds)).balanceReplicas(replicaNodes = Nil,takeN=rf)
          val xs = selectedNodeIds.map(nodeId => nodes.find(_.nodeId == nodeId)).sequence
          xs
        case "ROUND_ROBIN" =>
          val defaultCounter = nodeIds.map(x=>x->0).toList.toMap
          val lb      = deterministic.RoundRobin(nodeIds = nodeIds)
          val counter = Events.onlyPutos(events=events).map(_.asInstanceOf[Put]).filterNot(_.replication).groupBy(_.nodeId).map{
              case (nodeId,xs)=>nodeId -> xs.length
            }
          val pivotNode     = lb.balanceWith(nodeIds =nodeIds,counter = counter |+| defaultCounter)
          val selectedNodes = lb.balanceReplicas(
            replicaNodes = pivotNode::Nil,
            takeN =  rf-1
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
        case None =>ctx.logger.error("NO_SELECTED_NODE")*>Accepted()
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
      objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)

      maybeObject        = Events.getObjectByIdV3(objectId = objectId,operationId =operationId ,events=events)
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
      nN                 = arMap.size
      impactFactor       = authReq.req.headers.get(CIString("Impact-Factor")).flatMap(_.head.value.toDoubleOption).getOrElse(1/nN.toDouble)
      //   _______________________________________________________________________________
      response           <- maybeObject match {
        case Some(o) => alreadyUploaded(o,events=events)
        case None => maybeARNodeX match {
//          _________________________________________________
            case Some(nodes) => for {
              _             <- IO.unit
              replicaNodes  =  Events.getReplicasByObjectId(events = events,objectId = objectId)
              filteredNodes = nodes.filterNot(x=>replicaNodes.contains(x.nodeId))
              res           <- if(filteredNodes.isEmpty) ctx.logger.debug("NO_AVAIABLE_NODES") *> Accepted()
              else commonCode(operationId)(
                  objectId   = objectId,
                  objectSize = objectSize,
                  userId     = user.id,
                  events     = events,
                  nodes      = NonEmptyList.fromListUnsafe(filteredNodes),
                  rf         = math.floor(impactFactor*nN).toInt
                )
            } yield res
//          ____________________________________________________
            case None => ctx.logger.debug("NO_NODES,NO_LB") *> Forbidden()
          }
      }
    } yield response

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext)={
    AuthedRoutes.of[User,IO]{
//    __________________________________________________________________________________
      case authReq@POST -> Root / "upload" as user =>
        val defaultConvertion = (x:FiniteDuration) =>  x.toNanos
        val monotonic         = IO.monotonic.map(defaultConvertion)
        val program = for {
          serviceTimeStart   <- monotonic
          _                  <- s.acquire
          now                <- IO.realTime.map(defaultConvertion)
//      ______________________________________________________________________________________
          currentState       <- ctx.state.get
          rawEvents          = currentState.events
          events             = Events.orderAndFilterEventsMonotonicV2(rawEvents)
//      ______________________________________________________________________________________
          headers              = authReq.req.headers
          operationId          = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectId             = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectSize           = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
          fileExtension        = headers.get(CIString("File-Extension")).map(_.head.value).getOrElse("")
          filePath             = headers.get(CIString("File-Path")).map(_.head.value).getOrElse(s"$objectId.$fileExtension")
          compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head.value).getOrElse("")
          requestStartAt       = headers.get(CIString("Request-Start-At")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
          catalogId            = headers.get(CIString("Catalog-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          digest               = headers.get(CIString("Digest")).map(_.head.value).getOrElse("")
          blockIndex           = headers.get(CIString("Block-Index")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
          blockId              = s"${objectId}_${blockIndex}"
          latency              = serviceTimeStart - requestStartAt
            //      ______________________________________________________________________________________________________________
          _                  <- ctx.logger.debug(s"LATENCY $objectId $latency")
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
              serialNumber         = 0,
              objectId             = objectId,
              objectSize           = objectSize,
              timestamp            = now,
              nodeId               = selectedNodeId,
              serviceTimeNanos     = serviceTime,
              userId               = user.id,
              serviceTimeEnd       = serviceTimeEnd,
              serviceTimeStart     = serviceTimeStart,
              correlationId        = operationId,
              monotonicTimestamp   = 0L,
              blockId              = blockId,
              catalogId            = catalogId,
              realPath             = filePath,
              digest               = digest,
              compressionAlgorithm = compressionAlgorithm,
              extension            = fileExtension
            )
          }
          _                  <- Events.saveEvents(_events)
//      ______________________________________________________________________________________
          newResponse        = response.putHeaders(
            Headers(
              Header.Raw(CIString("Latency"),latency.toString),
              Header.Raw(CIString("Service-Time"),serviceTime.toString),
              Header.Raw(CIString("Service-Time-Start"), serviceTimeStart.toString),
              Header.Raw(CIString("Service-Time-End"), serviceTimeEnd.toString),
            )
          )
          _                  <- ctx.logger.debug("____________________________________________________")
          _                  <- s.release
        } yield newResponse
//      ______________________________________________________________________________________
        program.handleErrorWith{e=>
          ctx.logger.debug(e.getMessage)  *> InternalServerError()
      }
    }
  }

}
