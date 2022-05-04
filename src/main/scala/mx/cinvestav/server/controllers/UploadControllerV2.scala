package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.PendingSystemReplica
import mx.cinvestav.commons.types.PendingReplication
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

  def balance(events: List[EventX])(objectSize:Long,nodes:NonEmptyList[NodeX],rf:Int)(implicit ctx:NodeContext) = {
    val nodeIds            = nodes.map(_.nodeId)
    val filteredNodes      = nodes.filter(_.availableStorageCapacity >= objectSize)
    val filteredNodeIds    = filteredNodes.map(_.nodeId)
    val filteredUfs        = filteredNodes.map(_.ufs)

    if(filteredNodes.isEmpty) Option.empty[List[NodeX]]  else ctx.config.uploadLoadBalancer match {
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
  }


  def commonCode(operationId:String)(
    objectId:String,
    objectSize:Long,
    userId:String,
    events:List[EventX],
    nodes:NonEmptyList[NodeX],
    blockIndex:Int =0,
    rf:Int = 1,
    replicationTechique:String = "ACTIVE"
  )(implicit ctx:NodeContext) = {
    for {
      _                  <- IO.unit
      maybeSelectedNode  = balance(events= events)(objectSize = objectSize,nodes = nodes,rf = rf)
      response          <- maybeSelectedNode match {
        case Some(nodes)  =>
          val nNodes  = nodes.length
          val diffRF  = rf - nNodes

          val program = for {
              _        <- ctx.logger.debug(s"SELECTED_NODES $nodes")
              xs       <- nodes.zipWithIndex.traverse{
                case (node,index) =>
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
                          operationId = s"${operationId}_$blockIndex",
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
              hs       = xs.map(_._2.headers).flatten
              headers  = Headers(hs)
              //            .foldLeft(Headers.empty)(_ ++ _)
              balances = xs.map(_._1)
              res      <- Ok(balances.asJson,headers)
            } yield res

          ctx.logger.debug(s"RF $rf")*>ctx.logger.debug(s"DIFF_RF $diffRF") *> (diffRF match {
            case 0 => program
            case x =>
              for {
                _ <- IO.unit
                pendingSystemReplicas = PendingSystemReplica( rf = rf,ar=nodes.length,mandatory = false)
                pendingReplication    = PendingReplication(objectId = objectId, objectSize = objectSize,rf = x,replicationTechique)
                _                     <- ctx.state.update{ s=>
                  s.copy(
                    pendingReplicas       =  s.pendingReplicas + (objectId -> pendingReplication),
                    pendingSystemReplicas = s.pendingSystemReplicas:+ pendingSystemReplicas
                  )
                }
                res                 <- program
              } yield res
          })

        case None =>
          for {
//            maybeSystemRepREs <- ctx.config.systemReplication.launchNode().start
            res               <- Accepted()
            _ <- IO.unit
            pendingSystemReplicas = PendingSystemReplica( rf = rf,ar=0,mandatory = false)
            _                     <- ctx.state.update{ s=>
              s.copy(
                pendingSystemReplicas = s.pendingSystemReplicas:+ pendingSystemReplicas
              )
            }
            //            res <- maybeSystemRepREs match {
//              case Some(value) =>
//                val nodeId = value.nodeId
//                ctx.logger.debug(s"NEW_NODE_ADDED $nodeId")
//                Accepted()
//              case None => Accepted()
//            }
        }  yield res
      }
    } yield response
  }

  def controller(
                  operationId:String,
                  objectId:String,
                  blockIndex:Int = 0
                )(
    authReq:AuthedRequest[IO,User],
    user:User,
    rawEvents:List[EventX]=Nil,
    events:List[EventX]=Nil,
  )(implicit ctx:NodeContext) =
    for {
      currentState         <- ctx.state.get
      req                  = authReq.req
      headers              = req.headers
      objectSize           = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      replicationTechnique = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(currentState.replicationTechnique)

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
//      _ <- ctx.logger.debug(s"AR $")
      nN                 = arMap.size
      impactFactor       = authReq.req.headers.get(CIString("Impact-Factor"))
        .flatMap(_.head.value.toDoubleOption)
        .map(x=> if(x==0.0) ctx.config.defaultImpactFactor else x)
        .getOrElse(1/nN.toDouble)

      rf = authReq.req.headers.get(CIString("Replication-Factor"))
        .flatMap(_.head.value.toIntOption)
        .getOrElse(1)
//        .map()
      //   _______________________________________________________________________________
      uploadProgram = (nodes:NonEmptyList[NodeX]) => for {
          _             <- ctx.logger.debug(s"AR: ${nodes.length}")
          replicaNodes  =  Events.getReplicasByObjectId(events = events,objectId = objectId)
          filteredNodes = nodes.filterNot(x=>replicaNodes.contains(x.nodeId))
          res           <- if(filteredNodes.isEmpty) {
              for {
                 _                         <- ctx.logger.debug("SYSTEM_REPLICATION_ACTIVED")
                 maybeSystemReplicationRes <- ctx.config.systemReplication.launchNode().start

                res <- Accepted()
              } yield res
          } else commonCode(operationId)(
            objectId   = objectId,
            objectSize = objectSize,
            userId     = user.id,
            events     = events,
            nodes      = NonEmptyList.fromListUnsafe(filteredNodes),
            rf         =if(impactFactor == 0.0 ) rf  else math.ceil(impactFactor*nN).toInt,
            blockIndex = blockIndex,
            replicationTechique = replicationTechnique
          )
        } yield res

      response           <- maybeObject match {
        case Some(o) => maybeARNodeX match {
          case Some(nodes) => alreadyUploaded(o,events = events) *> uploadProgram(nodes)
          case None => Accepted()
        }
        case None => maybeARNodeX match {
//          _________________________________________________
            case Some(nodes) => uploadProgram(nodes)
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
          arrivalTime          = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
          catalogId            = headers.get(CIString("Catalog-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          digest               = headers.get(CIString("Digest")).map(_.head.value).getOrElse("")
          blockIndex           = headers.get(CIString("Block-Index")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
          blockId              = s"${objectId}_${blockIndex}"
          latency              = serviceTimeStart - requestStartAt
            //      ______________________________________________________________________________________________________________
          _                    <- ctx.logger.debug(s"LATENCY $objectId $latency")
          _                    <- ctx.logger.debug(s"ARRIVAL_TIME $objectId $serviceTimeStart")
//      ______________________________________________________________________________________________________________
          _                    <- ctx.logger.debug(s"SERVICE_TIME_START $objectId $serviceTimeStart")
//        ___________________________________________________________________________________
          response             <- controller(
            operationId = operationId,
            objectId    = objectId,
            blockIndex  = blockIndex
          )(authReq=authReq,user=user, rawEvents=rawEvents, events=events)
//        _____________________________________________________________
          serviceTimeEnd     <- monotonic
          serviceTime        = serviceTimeEnd - serviceTimeStart
//       _______________________________________________________________________________
          headers            = response.headers
          selectedNodeIds    = headers.get(CIString("Node-Id")).map(x=>x.toList.map(_.value)).getOrElse(List.empty[String])
          _                  <- ctx.logger.debug(s"SERVICE_TIME_END $objectId $serviceTimeEnd")
          _                  <- ctx.logger.debug(s"SERVICE_TIME $objectId $serviceTime")
//      ______________________________________________________________________________________
          _events            = selectedNodeIds.zipWithIndex.map{
            case (selectedNodeId,index) =>
            Put(
              replication          = false,
              serialNumber         = 0,
              objectId             = objectId,
              objectSize           = objectSize,
              timestamp            = now,
              nodeId               = selectedNodeId,
              serviceTimeNanos     = serviceTime,
              userId               = user.id,
              serviceTimeEnd       = serviceTimeEnd,
              serviceTimeStart     = serviceTimeStart,
              correlationId        = s"${operationId}_$blockIndex",
              monotonicTimestamp   = 0L,
              blockId              = blockId,
              catalogId            = catalogId,
              realPath             = filePath,
              digest               = digest,
              compressionAlgorithm = compressionAlgorithm,
              extension            = fileExtension,
              arrivalTime          = arrivalTime
            )
          }
//          _ <- ctx.logger.debug(_events.toString)
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
