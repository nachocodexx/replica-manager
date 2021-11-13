package mx.cinvestav
import breeze.linalg._
import breeze.stats.{mean, stddev}
import retry.retryingOnFailuresAndAllErrors
import retry.{RetryDetails, RetryPolicies}
//import breeze.stats.distributions.
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.balancer.v3.UF
import mx.cinvestav.commons.events.{EventX, EventXOps, Evicted, Get, Put, Replicated,TransferredTemperature}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events

import java.util.UUID
//
import mx.cinvestav.Declarations.{NodeContext, ObjectId,CreateNodeResponse}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.balancer.v3.{RoundRobin,PseudoRandom,Balancer=>BalancerV3,TwoChoices}
import mx.cinvestav.server.HttpServer.{PushResponse, ReplicationResponse}
//import mx.cinvestav.commons.t
//
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.typelevel.ci.CIString
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
//
import concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps

object Helpers {


  def serviceReplicationDaemon(period:FiniteDuration = 1 second )(implicit ctx:NodeContext): IO[Unit] = {
    if(ctx.config.serviceReplicationDaemon)  {
      val app = for {

        currentState  <- ctx.state.get
//        SEMAPHORE
        _             <- currentState.systemSemaphore.acquire
        threshold     = currentState.serviceReplicationThreshold
        rawEvents     = currentState.events
        events        = Events.filterEventsMonotonic(events=rawEvents)
        nodes         = Events.getAllNodeXs(events=events)
        nodesLen      = nodes.length
        _             <- if(nodesLen < currentState.maxAR && nodesLen>0) for {
          _      <- IO.unit
          ufs    = nodes.map(x=>UF.calculate(total =x.cacheSize,used= x.usedCacheSize,objectSize=0))
          avgUfs = ufs.sum/nodesLen

          _      <- ctx.logger.debug(s"AVG_CACHE_SIZE_UF = $avgUfs")
          signalValue <- currentState.systemRepSignal.get
          _      <- ctx.logger.debug(s"SYSTEM_REP_SIGNAL $signalValue")
          _      <- if(avgUfs >= threshold  & !signalValue) for {
            _ <- currentState.systemRepSignal.set(true)
            _ <- Helpers.createNode().flatMap{ x =>
              ctx.logger.debug(s"CREATED_NODE ${x.map(_.nodeId)}")
            }.start.void
          } yield ()
          else IO.unit
        } yield ()
//     ______________________________________________________
        else IO.unit
        _             <- currentState.systemSemaphore.release.delayBy(5 seconds)
      } yield ()


      fs2.Stream.awakeEvery[IO](period=period).evalMap{ _ =>app}.compile.drain
    }
    else IO.unit
  }

  def createNode()(implicit ctx:NodeContext): IO[Option[CreateNodeResponse]] = {
    for {
      _                  <- ctx.logger.debug(s"INIT_NODE_CREATION")
      currentState       <- ctx.state.get
      rawEvents          = currentState.events
      events             = Events.filterEventsMonotonic(events=rawEvents)
      nodesLen           = Events.onlyAddedNode(events=events).length
      signalSysRep       = currentState.systemRepSignal
      signalValue        <- signalSysRep.get
      systemRepResponse  <- if(nodesLen < currentState.maxAR && !signalValue) for {
        _                <- signalSysRep.set(true)
        systemRepPayload   = Json.obj(
          "poolId"->ctx.config.poolId.asJson,
          "cacheSize" -> ctx.config.defaultCacheSize.asJson,
          "policy" -> ctx.config.defaultCachePolicy.asJson,
          "basePort" -> ctx.config.defaultCachePort.asJson,
          "networkName" -> "my-net".asJson,
          "image" -> Json.obj(
            "name"->"nachocode/cache-node".asJson,
            "tag" -> "ex0".asJson
          ),
          "environments" -> Map(
            "POOL_HOSTNAME" -> ctx.config.nodeId.asJson,
            "POOL_PORT" -> ctx.config.port.toString.asJson
          ).asJson
        )
        systemRepReq       = Request[IO](
          method = Method.POST,
          uri = ctx.config.systemReplication.createNodeUri,
          headers = Headers(
            Header.Raw(CIString("Pool-Node-Length"), nodesLen.toString),
            Header.Raw(CIString("Host-Log-Path"),ctx.config.hostLogPath),
            Header.Raw(CIString("Max-AR"),currentState.maxAR.toString)
          )
        ).withEntity(systemRepPayload)

        (client,finalizer)  <- BlazeClientBuilder[IO](global).resource.allocated
        retryPolicy         = RetryPolicies.limitRetries[IO](5) join RetryPolicies.exponentialBackoff(1 second)
//        systemRepResponseIO = client.expect[CreateNodeResponse](systemRepReq)
        systemRepResponseIO = client.toHttpApp.run(systemRepReq)
        systemRepResponsev2   <- retryingOnFailuresAndAllErrors[Response[IO]](
          policy = retryPolicy,
          onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage) *> ctx.errorLogger.error(d.toString),
          onFailure= (response:Response[IO],d:RetryDetails) => ctx.errorLogger.error(d.toString),
          wasSuccessful= (response:Response[IO])=> (response.status.code == 204).pure[IO]
        )(systemRepResponseIO)
        systemRepResponse    <- if(systemRepResponsev2.status.code == 200)  systemRepResponsev2.as[CreateNodeResponse].map(_.some)
        else Option.empty[CreateNodeResponse].pure[IO]
//          systemRepResponse   <- retryingOnAllErrors[CreateNodeResponse](
//          policy = retryPolicy,
//          onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage),
//          //        isWorthRetrying = (e:Throwable) => IO.pure(true)
//        )(systemRepResponseIO)
//        _                  <- ctx.logger.debug(s"NODE_CREATED ${systemRepResponse.nodeId}")
//        _                  <- ctx.logger.debug(systemRepResponse.toString)
        _                  <- finalizer
//        __________________________________________
        _                  <- ctx.logger.debug("SYSTEM_REP_SIGNAL OFF")
      } yield systemRepResponse
      else Option.empty[CreateNodeResponse].pure[IO]
      _                  <- currentState.systemRepSignal.set(false)
    } yield systemRepResponse
  }

  //
  def uploadSameLogic(userId:String,
                      nodeId: String,
                      currentNodeId:String,
                      objectId:String,
                      objectSize:Long,
                      arrivalTimeNanos:Long,
                        response:Response[IO],
                        correlationId:String = UUID.randomUUID().toString
                       )(implicit ctx:NodeContext): IO[Unit] =
      {
        for {
          //        uploadServiceTime      <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
          uploadServiceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
          responseHeaders        = response.headers

          maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
          maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)
//          correlationId          = UUID.randomUUID().toString
          _                      <- ctx.logger.debug("EVICTED: "+maybeEvictedObjectId.toString)
          _                      <- maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize) match {
            case Some((evictedObjectId,evictedObjectSize)) => for {
              now       <- IO.realTime.map(_.toMillis)
              _         <- IO.unit
              newEvents = List(
               Put(
                serialNumber = 0,
                objectId = objectId,
                objectSize = objectSize,
                timestamp = now,
                nodeId = nodeId,
                serviceTimeNanos = uploadServiceTimeNanos,
                userId =  userId,
                correlationId = correlationId,
                monotonicTimestamp = 0L
              ),
                Evicted(
                eventId = UUID.randomUUID().toString,
                serialNumber = 0,
                nodeId = currentNodeId,
                objectId =  evictedObjectId,
                objectSize = evictedObjectSize,
                fromNodeId = nodeId,
                timestamp = now-10,
                serviceTimeNanos = 1,
                correlationId = correlationId,
                monotonicTimestamp = 0L
              )
              )
              _         <- Events.saveEvents(events = newEvents)
            } yield ()
            case None => for {
                now <- IO.realTime.map(_.toMillis)
                _   <- Events.saveEvents(
                  events =  List(
                    Put(
                      serialNumber = 0,
                      objectId = objectId,
                      objectSize = objectSize,
                      timestamp = now,
                      nodeId = nodeId,
                      serviceTimeNanos = uploadServiceTimeNanos,
                      userId =  userId,
                      correlationId = correlationId,
                      monotonicTimestamp = 0L
                )
              )
              )
              } yield ()
          }


        } yield ()
      }


  def balanceTemperature(objectId:String,replicaNodes:List[String],downloadsPerNode:Int,now:Long): List[TransferredTemperature] = {
    replicaNodes.zipWithIndex.map{
      case (replicaNode,index) =>
      TransferredTemperature(
        serialNumber = index,
        nodeId = replicaNode,
        objectId = objectId,
        counter = downloadsPerNode,
        timestamp = now+index,
        serviceTimeNanos = 1,
      )

    }
  }
  //  _________________________________
  def replicationDaemon(period:FiniteDuration= 10 seconds)(implicit ctx:NodeContext): IO[Unit] = {
    val app = for {
        _              <- ctx.logger.debug("<---REPLICATION DAEMON v.6--->")
        //       Get current state
        currentState   <- ctx.state.get
        //
        currentNodeId  = ctx.config.nodeId
        eventCount     = currentState.events.length
        //      interpreter events
        //          events         = Events.orderAndFilterEvents(events=currentState.events)
        events         = Events.orderAndFilterEventsMonotonic(events=currentState.events)
        //            Events.filterEvents(EventXOps.OrderOps.byTimestamp(currentState.events))
        //      Generate temp matrix from events
        tempMatrix     = Events.generateTemperatureMatrixV2(events = events)
        //
        _              <- if(tempMatrix.size ==0) IO.unit
        else {
          for {
            _                        <- IO.unit
            maybeBalancer            = currentState.uploadBalancer

            //                .getOrElse(UF())
            orderedObjectIds         = Events.getObjectIds(events=events).sorted
            objectIdsVec             = DenseVector(orderedObjectIds:_*)
            //            orderedNodeIds           = Events.getNodeIds(events=events)
            tempVec                  = mean(tempMatrix(::,*)).t
            meanThreshold            = mean(tempVec)
            stdDev                   = stddev(tempVec)
            //              zScores                  = (tempVec - meanThreshold) / stdDev
            _                        <- ctx.logger.debug(s"MEAN $meanThreshold")
            //              _                        <- ctx.logger.debug(s"STDDEV $stdDev")
            //            _                        <- ctx.logger.debug(s"Z-SCORE $zScores")
            thresholdTemperatureMask = tempVec >:> meanThreshold
            replicatedObjects        = objectIdsVec(thresholdTemperatureMask).toArray.toList
            _   <- if(replicatedObjects.isEmpty) ctx.logger.debug(s"No objects that are greater than $meanThreshold")
            else
            {
              for {
                _                     <- IO.unit
                //                  ars                   = Events.getAllNodeXs(events = events)
                ars                   = Events.getAllNodeXs(events = currentState.events.sortBy(_.monotonicTimestamp))
                numberOfNodes         = ars.length
                arMap                 = ars.map(node => node.nodeId -> node).toMap
                filteredARByCacheSize = ars.filter(_.availableCacheSize>0)
                isSystemRepActive <- currentState.systemRepSignal.get
                _                 <- ctx.logger.debug(s"SYSTEM_REP_SIGNAL $isSystemRepActive")
                _                 <- if(filteredARByCacheSize.isEmpty && !isSystemRepActive) {
                  for {
//                    _ <-
//                    _ <- if((numberOfNodes < currentState.maxAR) && ctx.config.serviceReplicationDaemon)  for {
                      _ <- ctx.logger.debug("CREATE_STORAGE_NODE")
                      _ <- createNode().start.void
//                    } yield ()
//                    else IO.unit
                  } yield ()
                }
                else {
                  for {
                    _                  <- IO.unit
                    //            GET ONLY REPLICATED OBJECTS
                    distributionSchema = Events.generateDistributionSchema(events=events)
                    filteredDistributionSchema = distributionSchema.filter{
                      case (objectId, value) => replicatedObjects.contains(objectId)
                    }
                    //              _____________________________________________________________________________-
                    //            OBJECT AND THE AVAILABLE NODES TO REPLICATE
                    availableNodesToReplicate = filteredDistributionSchema.map{
                      case (objectId, replicas) =>
                        objectId -> filteredARByCacheSize.map(_.nodeId).toSet.diff(replicas.toSet).toList
                    }.map{
                      case (objectId, nodeIds) =>
                        objectId -> nodeIds.map(arMap)
                    }.filter(_._2.nonEmpty).map{
                      case (str, value) =>  str-> NonEmptyList.fromListUnsafe(value)
                    }
                    //
                    //                      _ <- ctx.logger.debug("AVAILABLE_NODES_REPLICATE "+availableNodesToReplicate.map(x=>(x._1->x._2.map(_.nodeId))).asJson.toString)
                    //            _______________________________________________________________________________________
                    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
                    filteredMaxRF = availableNodesToReplicate.filter{
                      case (objectId, noReplicaNodes) => (numberOfNodes - noReplicaNodes.length) < currentState.maxRF
                    }
//                    replicateObject  = availableNodesToReplicate.headOption
                    replicateObject  = filteredMaxRF.headOption
                    x  <-  replicateObject match {
                      case Some((objectId, nodeCandidates)) =>

                        val maybeObjectSize   = EventXOps.onlyPuts(events=events)
                          .map(_.asInstanceOf[Put])
                          .find(_.objectId == objectId)
                          .map(_.objectSize)
                        // SELECT A NODE
                        val maybeSelectedNodeAndObjectSize = maybeObjectSize.mproduct(_=>maybeBalancer)
                          .flatMap{
                            case (size,lbb) =>
                              lbb.balance(objectSize=size,nodes=nodeCandidates).map(n=>(lbb,n,size))
                          }

                        for {
                          _ <- maybeSelectedNodeAndObjectSize match {
                            case Some((lbb,selectedNode,replicateObjectSize)) =>  for {
                              _          <- ctx.logger.debug(s"REPLICATED $objectId ${selectedNode.nodeId}")
                              //                                now        <- IO.realTime.map(_.toMillis)
                              //                                pullNodeId = distributionSchema(objectId).head
                              //                                pullNodeId_ = lb
                              //                                  lbb.balance(
                              //                                  objectSize = replicateObjectSize,
                              //                                  NonEmptyList.fromListUnsafe(distributionSchema(objectId))
                              //                                )
                              //
                              _             <- ctx.logger.debug(s"WARNING_AREA $objectId")
                              pullNodesIds  = distributionSchema(objectId)
                              _             <- ctx.logger.debug(s"PULL_NODE_IDS $pullNodesIds")
                              pullNodesXRaw = Events.getNodesByIsd(pullNodesIds,events=events)
//                              _             <- ctx.logger.debug(s"PULL_NODES_RAW $pullNodesXRaw")
                              pullNodesX    = NonEmptyList.fromListUnsafe(pullNodesXRaw)
                              pullArMap     = pullNodesX.map(x=>x.nodeId->x).toList.toMap
                              pullNodeX     = Events.balanceByReplica(currentState.downloadBalancerToken)(guid = objectId,arMap =pullArMap ,events=events).get
                              pullNodeId    = pullNodeX.nodeId
                              _ <- ctx.logger.debug(s"WARNING_AREA_SUCCESS $objectId")
                              //                                pullNodeId = distributionSchema(objectId).head
                              //                                pullFrom   = arMap(pullNodeId).httpUrl+s"/api/v6/download/$objectId"
                              pullFrom   = pullNodeX.httpUrl+s"/api/v6/download/$objectId"
                              unsafeUri  = s"${selectedNode.httpUrl+"/pull"}"
                              //
                              req      = Request[IO](
                                method = Method.POST,
                                uri = Uri.unsafeFromString(unsafeUri),
                                headers = Headers(
                                  Header.Raw(CIString("Pull-From"),pullFrom)
                                )
                              )
                              pullResponse <- client.toHttpApp.run(req)
                              pullHeaders  = pullResponse.headers
                              //
                              //                          _ <- ctx.logger.debug("BEFORE STs")
                              //
                              downloadSt             = pullHeaders.get(CIString("Download-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                              uploadSt               = pullHeaders.get(CIString("Upload-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                              pullSt                 = pullHeaders.get(CIString("Pull-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                              pullFromNodeId         = pullHeaders.get(CIString("Node-Id")).map(_.head.value).get

                              maybeEvictedObjectId   = pullHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
                              maybeEvictedObjectSize = pullHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)
                              pullEndAt              <- IO.realTime.map(_.toMillis)
                              pullEndAtNanos         <- IO.monotonic.map(_.toNanos)
                              // ___________________________________________________________________
                              evictedEvents <- maybeEvictedObjectId.mproduct(_ => maybeEvictedObjectSize) match {
                                case Some((evictedObjectId,evictedObjectSize)) => for {
                                  _            <- IO.unit
                                  evictedEvent = Evicted(
                                    eventId = UUID.randomUUID().toString,
                                    serialNumber = 0,
                                    nodeId = currentNodeId,
                                    objectId =  evictedObjectId,
                                    objectSize = evictedObjectSize,
                                    fromNodeId = selectedNode.nodeId,
                                    timestamp = pullEndAt+100,
                                    serviceTimeNanos = 1
                                  )
                                } yield List(evictedEvent)
                                case None => IO.pure(List.empty[EventX])
                              }
                              // ___________________________________________________________________
                              replicationServiceTimeNano <- IO.monotonic.map(_.toNanos).map(_ - pullEndAtNanos)
                              //                              BALANCE TEMPERATURE
                              balanceTemperatureEvents <- if(ctx.config.balanceTemperature) {
                                for {
                                  _ <- IO.unit
                                  downloads        = Events.getDownloadsByObjectId(objectId = objectId,events = events)
                                  dowloadsCounter  = downloads.length
                                  replicaNodesIds  = Events.getReplicaNodeIdsByObjectId(getsEvents = downloads)
                                  totalOfReplicas  = replicaNodesIds.length
                                  downloadsPerNode = math.ceil((dowloadsCounter/(totalOfReplicas+1).toDouble))
                                  _                <- ctx.logger.debug(s"REPLICAS $totalOfReplicas")
                                  _                <- ctx.logger.debug(s"DOWNLOADS_TOTAL $dowloadsCounter")
                                  _                <- ctx.logger.debug(s"DOWNLOADS_PER_NODE $downloadsPerNode")
                                  tempEvents       = balanceTemperature(objectId,replicaNodesIds,downloadsPerNode.toInt,pullEndAt+300)
                                } yield tempEvents
                              }
                              else List.empty[EventX].pure[IO]
                              //
                              //                                newEvents = EventXOps.secuential(
                              //                                  initialSerialNumber= eventCount,
                              _ <- Events.saveEvents(
                                events = List(
                                  Replicated(
                                    serialNumber     = eventCount,
                                    nodeId           = selectedNode.nodeId,
                                    objectId         = objectId,
                                    objectSize       = replicateObjectSize,
                                    replicas         = List(selectedNode.nodeId),
                                    rf               = 1,
                                    timestamp        = pullEndAt,
                                    serviceTimeNanos = replicationServiceTimeNano
                                  ),
                                  Put(
                                    serialNumber = eventCount+2,
                                    objectId = objectId,
                                    objectSize = replicateObjectSize,
                                    nodeId = selectedNode.nodeId,
                                    timestamp = pullEndAt+200,
                                    serviceTimeNanos = uploadSt,
                                    userId  = "SYSTEM_REP_DAEMON"
                                  ),
                                  Get(
                                    serialNumber = eventCount+1,
                                    objectId = objectId,
                                    objectSize = replicateObjectSize,
                                    nodeId = pullNodeId,
                                    timestamp = pullEndAt+300,
                                    serviceTimeNanos = downloadSt,
                                    userId  = "SYSTEM_REP_DAEMON"
                                  ),
                                ) ++ evictedEvents ++ balanceTemperatureEvents
                              )
                              //
                              //                                _ <- ctx.state.update(s=>s.copy(events = s.events ++ newEvents))
                              //                              ____________________________________________________________________
                            } yield ()
                            case None => ctx.logger.debug(s"NO AVAILABLE RESOURCES TO REPLICATE $objectId")
                          }
                          //                            _ <- IO.sleep(1 second)
                        } yield ()
                      case None => IO.unit
                    }
                    //            _______________________________________________________________________________________

                    _ <- finalizer
                    //                      RELEASE SEMAPHORE
                    //                      _  <- currentState.s.release
                  } yield ()
                }
              } yield ()
            }

          } yield ()
        }
        _ <- ctx.logger.debug("____________________________________________")
      } yield ( )
    if(ctx.config.replicationDaemon ) fs2.Stream.awakeEvery[IO](period=period) .evalMap(_ => app).compile.drain.onError{ e=>
        ctx.errorLogger.error(e.getMessage)  *> ctx.errorLogger.error(e.getStackTrace.mkString("Array(", ", ", ")"))
    }
    else IO.unit
  }

  def initLoadBalancerV3(balancerToken:String="UF")(implicit ctx:NodeContext): IO[BalancerV3] = for {
    _       <- IO.unit
    newLbV3 = balancerToken match {
      case  "UF"=> UF()
      case  "TWO_CHOICES"=> TwoChoices()(nodeXOrder)
      case "ROUND_ROBIN" => RoundRobin(forOperation = "UPLOAD_REQUESTS")(nodeXOrder)
      case "PSEUDO_RANDOM" => PseudoRandom()(nodeXOrder)
      case _ => UF()
    }
    _      <- ctx.state.update(s=>
      s.copy(
        uploadBalancer = newLbV3.some
      )
    )
  } yield newLbV3
//  def initLoadBalancer(loadBalancerType:String="LC",
//                       nodes:NonEmptyList[NodeX])(implicit ctx:NodeContext): IO[Balancer[NodeX]] = for {
//    _       <- IO.unit
//    newLb   = LoadBalancer[NodeX](loadBalancerType,xs= nodes)
////    newLbV3 = PseudoRandom()
//    _      <- ctx.state.update(s=>
//      s.copy(
//        lb =  newLb.some,
////        uploadBalancer = newLbV3.some
//      )
//    )
//  } yield newLb

//  Update node metadata -> update schema
  def commonLogic(arrivalTime:Long,req:Request[IO])(selectedNode:NodeX)(implicit ctx:NodeContext): IO[Response[IO]] ={
      for {
        _              <- IO.unit
//        headers        = req.headers
        selectedNodeId = selectedNode.nodeId
        _              <- ctx.logger.debug(s"CACHE_NODE_REQ $req")
        response       <-Helpers.redirectTo(selectedNode.httpUrl,req)
        payload        <- response.as[PushResponse]
//        payloadGUID    = ObjectId(payload.guid)
        //
//        responseHeaders  = response.headers
//        maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
//        maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)

        //          UPDATE CAPACITY OF A CACHE NODE
        //         Update schema based on eviction
//        _ <- maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize) match {
//          case Some( ( evictedObjectId,evictedObjectSize)) =>  for {
//            _              <- ctx.logger.debug(s"EVICTION $evictedObjectId of size $evictedObjectSize")
//            _              <- ctx.state.update{ s=>
//              val evicted = Evicted(
//                eventId = UUID.randomUUID().toString,
//                serialNumber = s.events.length,
//                nodeId = ctx.config.nodeId,
//                objectId = evictedObjectId,
//                objectSize = evictedObjectSize,
//                fromNodeId = selectedNodeId,
//                timestamp = arrivalTime,
//                milliSeconds = 1
//              )
//              s.copy(events =  s.events :+ evicted)
//            }
//          } yield ()
//          case None => ctx.logger.debug("NO EVIcTION!")
//        }

        endAt         <- IO.realTime.map(_.toMillis)
        time          = endAt - arrivalTime
        _             <- ctx.logger.info(s"UPLOAD ${payload.guid} ${payload.objectSize} ${selectedNode.nodeId} $time")
      } yield response
    }

  def placeReplicaInAR(arrivalTime:Long,
                       arMap:Map[String,NodeX],
                       lb:Balancer[NodeX],
                       req:Request[IO]
                      )
                      (objectId: ObjectId,
                       availableResources:List[String])
                      (implicit ctx:NodeContext): IO[Response[IO]] =
    for {
      _                           <- ctx.logger.debug(s"THERE ARE FREE SPACES $availableResources")
      headers                     = req.headers
      maybeGuid                   = headers.get(CIString("guid")).map(_.head.value)
      subsetNodes                 = NonEmptyList.fromListUnsafe(availableResources.map(x=>arMap(x)))
      rf                          = 1
      (newBalancer,selectedNodes) = Balancer.balanceOverSubset(balancer = lb, rounds= rf ,subsetNodes =subsetNodes)
      _                           <- ctx.logger.debug(s"SELECTED_NODES $selectedNodes")
      _                           <- ctx.state.update(s=>s.copy(lb = newBalancer.some))
      fiber                       <- selectedNodes.traverse(Helpers.commonLogic(arrivalTime=arrivalTime,req=req)(_))
      endAt                       <- IO.realTime.map(_.toMillis)
      milliSeconds                = endAt - arrivalTime
      payloadRss = ReplicationResponse(
        guid = objectId.value,
        replicas= selectedNodes.toList.map(_.nodeId),
        milliSeconds =  milliSeconds ,
        timestamp = arrivalTime
      )
      _res  <- Ok(payloadRss.asJson)
    } yield _res

  //  Create a SN and update the schema

    def redirectToWithClient(client:Client[IO])(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
      newReq = req.withUri(
        Uri.unsafeFromString(nodeUrl)
          .withPath(req.uri.path)
      )
//      _ <- IO.println(s"NEW_REQ $newReq")
      response <- client.toHttpApp.run(newReq)
    } yield response

    def redirectTo(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
  //    newReq = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
      (client, finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
      response            <- redirectToWithClient(client)(nodeUrl=nodeUrl,req=req)
  //    response <- client.toHttpApp.run(newReq)
      _ <- finalizer
    } yield response
}

