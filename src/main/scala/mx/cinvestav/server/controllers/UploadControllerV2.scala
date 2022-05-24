package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.{PendingSystemReplica, QueueOperation, UploadHeadersOps}
import mx.cinvestav.commons.types.PendingReplication
import org.http4s.Response
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.types.DumbObject
//
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.{NodeX,BalanceResponse}
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
    pivotReplicaNode:NodeX,
    //    nodexs:NonEmptyList[NodeX],
    blockIndex:Int =0,
    rf:Int = 1,
    _replicationTechique:String = "ACTIVE",
    replicaNodes:List[NodeX] = Nil,
    collaborative:Boolean = false,
  )(implicit ctx:NodeContext) = {
    for {
      _                  <- IO.unit
      maybeSelectedNode  = if(collaborative && replicaNodes.nonEmpty)  replicaNodes.some else balance(events= events)(objectSize = objectSize,nodes = nodes,rf = rf)
      response          <- maybeSelectedNode match {
        case Some(nodes)  =>
          val nNodes  = nodes.length
          val diffRF  = rf - nNodes

          val program = for {
              _        <- ctx.logger.debug(s"SELECTED_NODES ${nodes.map(_.nodeId)}")
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
                pendingReplication    = PendingReplication(
                  objectId             = objectId,
                  objectSize           = objectSize,
                  rf                   = x,
                  replicaNodes         = replicaNodes.map(_.nodeId),
                  replicationTechnique = _replicationTechique
                )
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

        }  yield res
      }
    } yield response
  }

  def controller(
                  operationId:String,
                  objectId:String,
                  pivotReplicaNode:NodeX,
                  blockIndex:Int = 0,
                  definedReplicaNodes:List[NodeX]=Nil,
                  collaborative:Boolean = false,
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

      maybeObject          = Events.getObjectByIdV3(objectId = objectId,operationId =operationId ,events=events)
      arMap                = ctx.config.uploadLoadBalancer match {
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
      maybeARNodeX         = NonEmptyList.fromList(arMap.values.toList)
      nN                   = arMap.size
      impactFactor         = authReq.req.headers.get(CIString("Impact-Factor"))
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
          }

          else commonCode(operationId)(
            objectId            = objectId,
            objectSize          = objectSize,
            userId              = user.id,
            events              = events,
            nodes               = NonEmptyList.fromListUnsafe(filteredNodes),
            rf                  = if(collaborative) definedReplicaNodes.length else if(impactFactor == 0.0 ) rf  else math.ceil(impactFactor*nN).toInt,

            blockIndex          = blockIndex,
            _replicationTechique = replicationTechnique,
            replicaNodes        = definedReplicaNodes,
            collaborative       = collaborative,
            pivotReplicaNode    = pivotReplicaNode
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
          nodexsL            = EventXOps.getAllNodeXs(events = events)
          nodexs             = nodexsL.map(x=>x.nodeId-> x).toMap
//      ______________________________________________________________________________________
          headers            = authReq.req.headers
//      ______________________________________________________________________________________________________________
          uphs               <- UploadHeadersOps.fromHeaders(headers = headers)
          latency              = serviceTimeStart - uphs.requestStartAt
//       ______________________________________________________________________________
//          currentOperation   = currentState.currentOperation
//          enqueuedOperations = currentState.clientOperations.values.toList.flatten
          operations         = EventXOps.completedPutsByObjectId(events =events,objectId = uphs.objectId)
          maybeOperation     = operations.find(_.operationId == uphs.operationId)
          maybeOperationBySerial = operations.find(_.serialNumber == uphs.serialNumber)
          rex                <- maybeOperation match {
            case Some(value) => for {
              _      <- ctx.logger.debug(s"OPERATION_ALREADY_COMPLETED ${value.operationId} ${value.objectId} ${value.serviceTimeNanos}")
              res    <- Forbidden()
            } yield res
            case None => for{
              _                  <- IO.unit
              unavailableNodeIds = operations.map(_.nodeId)
              availableNodexs    = nodexsL.filterNot(nxs => unavailableNodeIds.contains(nxs.nodeId))
              unavailableNodexs  = nodexsL.filter(x=> unavailableNodeIds.contains(x.nodeId))
              isCompleted        = operations.nonEmpty
              pendingPuts        = EventXOps.onlyPendingPuts(events = events)
              pendingGets        = EventXOps.onlyPendingGets(events = events)
              pendingNodes       = (pendingPuts.map(_.nodeId) ++ pendingGets.map(_.nodeId)).distinct
              program            = {
                maybeOperationBySerial match {
                  case Some(value) =>
                    for {
                      _   <- ctx.logger.debug(s"OPERATION_ALREADY_COMPLETED ${value.operationId} ${value.objectId} ${value.serviceTimeNanos}")
                      res <- Forbidden()
                    } yield res
                  case None => for {
                    _                 <- IO.unit
                    pivotNode         = nodexs(uphs.pivotReplicaNode)
                    operation         = QueueOperation(arrivalTime= serviceTimeStart, operationId = uphs.operationId, serialNumber = uphs.serialNumber, objectId = uphs.objectId, clientId = uphs.clientId,pullOrPushFrom = pivotNode.nodeId)
                    pivotQueue        = currentState.nodeQueue(pivotNode.nodeId)
                    replicaNodes      = uphs.replicaNodes.map(nodexs)
                    replicaNodesQueue = uphs.replicaNodes.map( rn=> rn -> currentState.nodeQueue(rn)).toMap
//                    ADD TO QUEUE
//                    _ <- ctx.state.update{s=>
//                      val newQueueMap = Map(
//                        uphs.pivotReplicaNode -> List(operation),
//                      )
//                      val rnQueuesMap = uphs.replicaNodes.map(rn=> Map(rn->  List(operation) )).foldLeft(Map.empty[String,List[QueueOperation]]  )(_ |+| _)
//                      val nodesQueue = newQueueMap |+| rnQueuesMap
//
//                      s.copy(nodeQueue = nodesQueue)
//                    }
                    res                <- Ok()
                  } yield res
                }

              }
//              _      <- if(operations.isEmpty) Forbidden()
              res <- program
            } yield res
          }
//        _______________________________________________________________________________________
//          _                  <- ctx.logger.debug(s"SERVICE_TIME_START ${uphs.objectId} $serviceTimeStart")
//          _                  <- ctx.logger.debug(s"COLLABORATIVE ${uphs.collaborative}")
//          _                  <- ctx.logger.debug(s"REPLICA_NODES ${uphs.replicaNodes}")
//          _                  <- ctx.logger.debug(s"ARRIVAL_TIME ${uphs.objectId} $serviceTimeStart")
//          latency            = serviceTimeStart - uphs.requestStartAt
//          _                  <- ctx.logger.debug(s"LATENCY ${uphs.objectId} ${latency}")
            //      ______________________________________________________________________________________________________________
//        ___________________________________________________________________________________
//          _ <- if(uphs.collaborative) {
//            val pr = PendingReplication(
//              objectId             = uphs.objectId,
//              objectSize           = uphs.objectSize,
//              rf                   = uphs.replicaNodes.length,
//              replicationTechnique = uphs.replicationTechnique,
//              replicaNodes         = uphs.replicaNodes
////                replicaNodes.toSet.diff(Set(pivotReplicaNode)).toList
//            )
//            ctx.state.update{
//              s=>{
//                val pendingReplicas    = s.pendingReplicas + (pr.objectId -> pr)
//                s.copy(pendingReplicas = pendingReplicas  )
//              }
//            }
//          } else IO.unit
//        _________________________________________
//          pivotReplicaNodex = nodexs(uphs.pivotReplicaNode)
//          response             <- controller(
//            operationId         = uphs.operationId,
//            objectId            = uphs.objectId,
//            blockIndex          = uphs.blockIndex,
//            definedReplicaNodes = uphs.replicaNodes.map(x=>nodexs.get(x) ).sequence.getOrElse(Nil),
//            collaborative       = uphs.collaborative,
//            pivotReplicaNode    = pivotReplicaNodex
//          )(authReq=authReq,user=user, rawEvents=rawEvents, events=events)
////        _____________________________________________________________
//          serviceTimeEnd     <- monotonic
//          serviceTime        = serviceTimeEnd - serviceTimeStart
////       _______________________________________________________________________________
//          selectedNodeIds    = List.empty[String]
////            headers.get(CIString("Node-Id")).map(x=>x.toList.map(_.value)).getOrElse(List.empty[String])
//          _                  <- ctx.logger.debug(s"SERVICE_TIME_END ${uphs.objectId} $serviceTimeEnd")
//          _                  <- ctx.logger.debug(s"SERVICE_TIME ${uphs.objectId} $serviceTime")
//      ______________________________________________________________________________________
//          _events            = selectedNodeIds.zipWithIndex.map {
//            case (selectedNodeId, index) => Put.fromUploadHeaders(
//              nodeId = selectedNodeId,
//              userId = user.id,
//              timestamp = now,
//              serviceTimeStart,
//              serviceTimeEnd,
//              uphs
//            )
//          }
//          selectedNodeIds    = List.empty[String]
//            if(uphs.collaborative) uphs.pivotReplicaNode::Nil else Nil
//          else headers.get(CIString("Node-Id")).map(x=>x.toList.map(_.value)).getOrElse(List.empty[String]) :+ uphs.pivotReplicaNode
//          _                  <- Events.saveEvents(_events)
//      ______________________________________________________________________________________
//          newResponse        = response.putHeaders(
//            Headers(
//              Header.Raw(CIString("Latency"),latency.toString),
//              Header.Raw(CIString("Service-Time"),serviceTime.toString),
//              Header.Raw(CIString("Service-Time-Start"), serviceTimeStart.toString),
//              Header.Raw(CIString("Service-Time-End"), serviceTimeEnd.toString),
//            )
//          )
          _                  <- ctx.logger.debug("____________________________________________________")
          _                  <- s.release
        } yield rex
//      ______________________________________________________________________________________
        program.handleErrorWith{e=>
          ctx.logger.debug(e.getMessage)  *> InternalServerError()
      }
    }
  }

}
