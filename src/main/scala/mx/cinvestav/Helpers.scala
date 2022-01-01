package mx.cinvestav
import breeze.linalg._
import breeze.stats.{mean, stddev}
import cats.effect.std.Semaphore
import fs2.concurrent.SignallingRef
import mx.cinvestav.events.Events.{MeasuredServiceTime, MonitoringStats}
import mx.cinvestav.replication.DataReplication
import retry.retryingOnFailuresAndAllErrors
import retry.{RetryDetails, RetryPolicies}

import java.time
//import breeze.stats.distributions.
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.balancer.v3.UF
import mx.cinvestav.commons.events.{EventX, EventXOps, Evicted, Get, Put, Replicated,TransferredTemperature}
import mx.cinvestav.commons.types.{NodeX,DumbObject}
import mx.cinvestav.events.Events
//import mx.cinvestav.commons.

import java.util.UUID
//
import mx.cinvestav.Declarations.{NodeContext, ObjectId,CreateNodeResponse}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.balancer.v3.{RoundRobin,PseudoRandom,Balancer=>BalancerV3,TwoChoices}
import mx.cinvestav.Declarations.{PushResponse, ReplicationResponse}
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


  def getMonitoringStatsFromHeaders(nodeId:String,arrivalTime:Long)(headers:Headers) = {
    val jvmTotalRAM   = headers.get(CIString("JVM-Total-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val jvmFreeRAM    = headers.get(CIString("JVM-Free-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val jvmUsedRAM    = headers.get(CIString("JVM-Used-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val jvmUfRAM      = headers.get(CIString("JVM-UF-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val totalRAM      = headers.get(CIString("Total-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val freeRAM       = headers.get(CIString("Free-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val usedRAM       = headers.get(CIString("Used-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val ufRAM         = headers.get(CIString("UF-RAM")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val systemCpuLoad = headers.get(CIString("System-CPU-Load")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val cpuLoad       = headers.get(CIString("CPU-Load")).flatMap(_.head.value.toDoubleOption).getOrElse(0.0)
    val newEvent      = MonitoringStats(
      serialNumber = 0,
      nodeId = nodeId,
      timestamp = arrivalTime,
      monotonicTimestamp = 0L,
      jvmTotalRAM = jvmTotalRAM,
      jvmFreeRAM = jvmFreeRAM,
      jvmUsedRAM = jvmUsedRAM,
      jvmUfRAM = jvmUfRAM,
      totalRAM = totalRAM,
      freeRAM = freeRAM,
      usedRAM = usedRAM,
      ufRAM = ufRAM,
      systemCPULoad = systemCpuLoad,
      cpuLoad = cpuLoad
    )
    newEvent
  }

  def serviceReplicationDaemon(s:SignallingRef[IO,Boolean],period:FiniteDuration = 1 second )(implicit ctx:NodeContext): IO[Unit] = {
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
          signalValue <- s.get
          _      <- ctx.logger.debug(s"SYSTEM_REP_SIGNAL $signalValue")
          _      <- if(avgUfs >= threshold  & !signalValue) for {
//            _ <- currentState.systemRepSignal.set(true)
            _ <- Helpers.createNode(s).flatMap{ x =>
              ctx.logger.debug(s"CREATED_NODE_SUCCESSFULLY")
            }.onError{ e=>
               ctx.logger.debug(s"CREATE_NODE_ERROR ${e.getMessage}")
            }
          } yield ()
          else IO.unit
          _ <- ctx.logger.debug("____________________________________________________")
        } yield ()
        //     ______________________________________________________
        else IO.unit
        _             <- currentState.systemSemaphore.release.delayBy(5 seconds)
      } yield ()


      fs2.Stream.awakeEvery[IO](period=period).evalMap{ _ =>app}.compile.drain
    }
    else IO.unit
  }

  def createNode(s:SignallingRef[IO,Boolean])(implicit ctx:NodeContext) = {
    for {
      _                  <- ctx.logger.debug(s"INIT_NODE_CREATION")
      currentState       <- ctx.state.get
      rawEvents          = currentState.events
      events             = Events.filterEventsMonotonicV2(events=rawEvents)
      nodesLen           = Events.onlyAddedNode(events=events).length
//      signalSysRep       = currentState.systemRepSignal
      signalValue        <- s.get
      _                  <- ctx.logger.debug(s"CURRENT_SIGNAL_VAL $signalValue")
      _                  <- ctx.logger.debug(s"NODES_LEN $nodesLen")
      _                  <- ctx.logger.debug(s"MAX_AR ${currentState.maxAR}")
      systemRepResponse  <- if(nodesLen < currentState.maxAR && !signalValue) for {
        _                <- s.set(true)
        systemRepPayload   = Json.obj(
          "poolId"->ctx.config.nodeId.asJson,
          "cacheSize" -> ctx.config.defaultCacheSize.asJson,
          "policy" -> ctx.config.defaultCachePolicy.asJson,
          "basePort" -> ctx.config.defaultCachePort.asJson,
          "networkName" -> "my-net".asJson,
          "image" -> Json.obj(
            "name"->"nachocode/cache-node".asJson,
            "tag" -> "v2".asJson
          ),
          "environments" -> Map(
            "POOL_HOSTNAME" -> ctx.config.nodeId.asJson,
            "POOL_PORT" -> ctx.config.port.toString.asJson
          ).asJson
        )
        _                  <- ctx.logger.debug("AFTER_PAYLOAD")
        uriStr = ctx.config.systemReplication.createNodeStr
        _ <- ctx.logger.debug(s"SYS_REP_STR_URI $uriStr")
        uri  =  Uri.unsafeFromString(uriStr)
//          ctx.config.systemReplication.createNodeUri
        _ <- ctx.logger.debug(s"SYS_REP_URI $uri")
        systemRepReq       = Request[IO](
          method = Method.POST,
          uri = uri,
          headers = Headers(
            Header.Raw(CIString("Pool-Node-Length"), nodesLen.toString),
            Header.Raw(CIString("Host-Log-Path"),ctx.config.hostLogPath),
            Header.Raw(CIString("Max-AR"),currentState.maxAR.toString)
          )
        ).withEntity(systemRepPayload)
        _                  <- ctx.logger.debug("AFTER_REQUEST")
          systemRepResponse <- ctx.client.stream(systemRepReq).evalMap{ res=>
            ctx.logger.debug(res.toString) *> ctx.logger.debug(s"SYS_REP_STATUS ${res.status}")
          }.compile.drain.onError(e=>ctx.logger.debug(s"ERROR ${e.getMessage}"))
        _                  <- ctx.logger.debug("AFTER_RESPONSE")
        _                   <- ctx.logger.debug(systemRepResponse.toString)
        _                  <- ctx.logger.debug("SYSTEM_REP_SIGNAL OFF")
      } yield ()
      else IO.unit
      _                  <- IO.sleep(10 seconds) *> s.set(false)
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
  def replicationDaemon(sReplication:Semaphore[IO], period:FiniteDuration= 10 seconds,signal:SignallingRef[IO,Boolean])(implicit ctx:NodeContext): IO[Unit] = {
    val app = for {
      currentState  <- ctx.state.get
      _             <- ctx.logger.debug(s"<---REPLICATION DAEMON[${currentState.replicationStrategy}]--->")
      rawEvents     = currentState.events
      events        = Events.orderAndFilterEventsMonotonicV2(events=rawEvents)
      dataReplicationStrategy  = currentState.replicationStrategy
      hotData       <- dataReplicationStrategy match{
        case "static" => Events.getDumbObject(events=events).pure[IO]
        case "v0"     => DataReplication.selectedHotV0(events=rawEvents)
        case "v1"     => DataReplication.selectHotDataV1(events)
        case "v2"     => DataReplication.selectHotDataV2(events)
        case "v3"     => DataReplication.selectHotDataV3(events).pure[IO].onError{ t=>
          ctx.errorLogger.error(t.getMessage)
        }
      }
      _             <- ctx.logger.debug(s"HOT_DATA_SIZE ${hotData.length}")
      _             <- if(hotData.isEmpty) ctx.logger.debug(s"No objects to replicate")
      else for {
        _   <- IO.unit
        ars = Events.getAllNodeXs(events = rawEvents.sortBy(_.monotonicTimestamp))
        _   <- sReplication.acquire
        _   <- DataReplication.run(ars=ars,events= events,hotData= hotData)
        _ <- sReplication.release.delayBy(100 milliseconds)
      } yield ()
      _ <- ctx.logger.debug("____________________________________________")
      } yield ( )

    for {
      currentState <- ctx.state.get
      _ <- if(currentState.replicationDaemon)
       fs2.Stream.awakeEvery[IO](period=period) .evalMap(_ => app)
      .interruptWhen(signal)
      .compile.drain.onError{ e=>
      ctx.errorLogger.error(e.getMessage)  *> ctx.errorLogger.error(e.getStackTrace.mkString("Array(", ", ", ")"))
    }
    else IO.unit
    } yield ()

  }

  def initLoadBalancerV3(balancerToken:String="UF")(implicit ctx:NodeContext): IO[BalancerV3] = for {
    _       <- IO.unit
    newLbV3 = balancerToken match {
      case  "UF"=> UF()(nodeXOrder)
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

