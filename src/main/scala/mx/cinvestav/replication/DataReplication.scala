package mx.cinvestav.replication

import breeze.linalg._
import breeze.stats.{mean, stddev}
import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers.balanceTemperature
import mx.cinvestav.commons.events.{EventX, EventXOps, Evicted, Get, Put, Replicated}
import mx.cinvestav.commons.types.{NodeX,DumbObject}
import mx.cinvestav.events.Events
import mx.cinvestav.events.Events.HotObject
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.http4s.blaze.client.BlazeClientBuilder
import org.typelevel.ci.CIString

import java.util.UUID
import scala.concurrent.ExecutionContext.global

import concurrent.duration._
import language.postfixOps
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.commons.types.DumbObject
object DataReplication {


  def getReplicaNodesOfHotData(events:List[EventX],hotObjectId:String,ars:List[NodeX])(implicit ctx:NodeContext):IO[Option[List[NodeX]]]= {
    for {
      currentState                <- ctx.state.get
      numberOfNodes               = ars.length
      distributionSchema          = Events.generateDistributionSchema(events=events)
//      filteredDistributionSchema  = distributionSchema.filter{ case (objectId, _) => objectId == hotObjectId}
      filteredDistributionSchema_ = distributionSchema.get(hotObjectId)
      //              _____________________________________________________________________________-
      res             <- filteredDistributionSchema_ match {
        case Some(replicaNodes) => for {
          _                     <- IO.unit
          arMap                 = ars.map(node => node.nodeId -> node).toMap
          filteredARByCacheSize = ars.filter(_.availableCacheSize>0)
          //     SET_NODES_WITH_AVAILABLE_CACHE > 0   -  REPLICA_NODES  = {NEW_REPLICA_NODES_CANDIDATES}
          availableNodesToReplicate =  filteredARByCacheSize.map(_.nodeId).toSet.diff(replicaNodes.toSet)
            .toList
            .map(arMap)
          maxRF = 3
          isLowerThanMaxRF  = (numberOfNodes - availableNodesToReplicate.length) < maxRF
          res =  if(isLowerThanMaxRF) availableNodesToReplicate.some
          else Option.empty[List[NodeX]]
        } yield res
        case None => None.pure[IO]
      }
    } yield res
  }

  def selectedHotV0(events:List[EventX])(implicit ctx:NodeContext):IO[List[DumbObject]] = {

    for {
      currentState       <- ctx.state.get
      maybeLastMonotonic = events.maxByOption(_.monotonicTimestamp).map(_.monotonicTimestamp)
      tenSecondsInNanos  = 0L
//        (ctx.config.dataReplicationIntervalMs milliseconds).toNanos
      windowTime         = maybeLastMonotonic.map(_ - tenSecondsInNanos).getOrElse(0L)
//      newEvents          = events.filter(_.monotonicTimestamp >  windowTime)
      matrix             = Events.generateMatrixV2(events=events,windowTime = windowTime)
//      ______________________________________________________________________
      hotObjects <- if(matrix.size ==0) List.empty[DumbObject].pure[IO]
      else {
        for {
          _                     <- IO.unit
          sumA                  = sum(matrix(::,*))
          _                     <- ctx.logger.debug(s"SUM $sumA")
          maxDownloadedObjIndex = argmax(sumA)
          _                     <- ctx.logger.debug(s"MAX_DOWNLOAD $maxDownloadedObjIndex")
          objects               = Events.getDumbObject(events = events).sortBy(_.objectId)
          ob_                   = objects.map(_.objectId).zipWithIndex
          _                     <- ctx.logger.debug(ob_.toString())
          maybeHotObj           = objects.get(maxDownloadedObjIndex)
          _                     <- ctx.logger.debug(s"MAYBE_HOT_DATA ${maybeHotObj}")
          hotObjects            <- maybeHotObj match {
             case Some(hotObject) => for {
               now            <- IO.realTime.map(_.toMillis)
               filteredHotObjectEvents =  maybeLastMonotonic.map{ lastMonotonic =>
                 events.filter{
                   case h:HotObject => h.monotonicTimestamp >= (lastMonotonic-tenSecondsInNanos)
                   case _=>false
                 }
               }
               x              <- filteredHotObjectEvents match {
                 case Some(hotObjectsEvents) => for {
                   _                   <- IO.unit
//                 GET NUMBER OF HOT
                   distributionSchema = Events.generateDistributionSchema(events=events).map(x=>x._1->x._2.length)
                   maxRF = 3
                   res <- distributionSchema.get(hotObject.objectId) match {
                     case Some(currentReplicationFactor) => if(currentReplicationFactor >= maxRF) List.empty[DumbObject].pure[IO]
                     else {
                       for {
                         _ <- IO.unit
                         hotCounter          = objects.map(x=> x.objectId -> Events.getHotObjectCounter(
                           objectId = x.objectId,
                           events = hotObjectsEvents,
                           lastMonotonic = 0
                           //                       windowTime
                         )).filter(_._2>0).toMap
                         //                 SUM UP ALL THE COUNTER OF HOTS
                         sumOfHotCounter     = hotCounter.values.sum
                         //                 GET THE MEAN
                         meanMaxNumberOfHots = sumOfHotCounter.toDouble/hotCounter.size.toDouble
                         filteredHotCounter = hotCounter.filter{
                           case (str, i) =>  i >= meanMaxNumberOfHots
                         }
                         _           <- ctx.logger.debug(s"SUM_COUNTER $sumOfHotCounter")
                         _           <- ctx.logger.debug(s"HOT_COUNTER_SIZE ${hotCounter.size}")
                         _           <- ctx.logger.debug(s"MEAN $meanMaxNumberOfHots")
                         _           <- ctx.logger.debug(filteredHotCounter.asJson.toString())
                         _           <- Events.saveEvents(events = List(
                           HotObject(serialNumber = 0,
                             nodeId = ctx.config.nodeId,
                             timestamp = now,
                             monotonicTimestamp = 0,
                             objectId = hotObject.objectId,
                             objectSize = hotObject.objectSize,
                             correlationId = UUID.randomUUID().toString
                           )
                         ))
                         res         = filteredHotCounter.maxByOption(_._2) match {
                           case Some(value) => objects.filter(_.objectId == value._1)
                           case None => List.empty[DumbObject]
                         }
                       } yield res
                     }
                     case None => List.empty[DumbObject].pure[IO]
                   }

//
                 } yield res
                 case None => List.empty[DumbObject].pure[IO]
               }

             } yield x
             case None => List.empty[DumbObject].pure[IO]
           }
        } yield hotObjects
      }
    } yield hotObjects
//    IO.pure(hotObjects)
  }
  def selectHotDataV1(events:List[EventX])(implicit ctx:NodeContext)= {
    for{
//      currentState   <- ctx.state.get
//      rawEvents      = currentState.events
      _ <- IO.unit
      maxTimeStamp   = events.maxByOption(_.timestamp).map(_.timestamp)
      windowTime     = 0
//      events         = Events.orderAndFilterEventsMonotonicV2(events=rawEvents)
      //      Generate temp matrix from events
      tempMatrix     = maxTimeStamp match {
        case Some(value) => Events.generateTemperatureMatrixV2(events = events,windowTime = windowTime)
        case  None => Events.generateTemperatureMatrixV2(events = events,windowTime = windowTime)
      }
      _                        <- IO.unit
      orderedDumbObjects       = Events.getDumbObject(events=events).sortBy(_.objectId)
      orderedObjectIds         = orderedDumbObjects.map(_.objectId)
//        Events.getObjectIds(events=events).sorted
      objectIdsVec             = DenseVector(orderedObjectIds:_*)
      tempVec                  = mean(tempMatrix(::,*)).t
      meanThreshold            = mean(tempVec)
      stdDev                   = stddev(tempVec)
      _                        <- ctx.logger.debug(s"MEAN $meanThreshold")
      thresholdTemperatureMask = tempVec >:> meanThreshold
      hotData                  = objectIdsVec(thresholdTemperatureMask).toArray.toList
      hotData_                 = orderedDumbObjects.filter(x=>hotData.contains(x.objectId))
    } yield hotData_
  }
  def selectHotDataV3(events:List[EventX])(implicit ctx:NodeContext)= {
    val F       = Events.getDumbObject(events=events)
    val period  =  1000 milliseconds
//      ctx.config.dataReplicationIntervalMs milliseconds
    val  x      = F.map{ f =>
        val y = Events.getDownloadsByIntervalByObjectId(objectId = f.objectId)(period)(events=events)
      (f -> DataReplication.nextNumberOfAccess(y))
    }
    x.filter(_._2>0).maxByOption(_._2) match {
      case Some(value) => value._1::Nil
      case None => Nil
    }
  }
  def selectHotDataV2(events:List[EventX])(implicit ctx:NodeContext): IO[List[DumbObject]] = {
    for{
      //      currentState   <- ctx.state.get
      //      rawEvents      = currentState.events
      _ <- IO.unit
//      maxTimeStamp   = events.maxByOption(_.timestamp).map(_.timestamp)
      windowTime     = 0
      //      events         = Events.orderAndFilterEventsMonotonicV2(events=rawEvents)
      //      Generate temp matrix from events
      tempMatrix     = Events.generateTemperatureMatrixV3(events=events,windowTime=0)
//        maxTimeStamp match {
//        case Some(value) => Events.generateTemperatureMatrixV2(events = events,windowTime = windowTime)
//        case  None => Events.generateTemperatureMatrixV2(events = events,windowTime = windowTime)
//      }
      _                        <- IO.unit
      orderedDumbObjects       = Events.getDumbObject(events=events).sortBy(_.objectId)
      orderedObjectIds         = orderedDumbObjects.map(_.objectId)
      //        Events.getObjectIds(events=events).sorted
      objectIdsVec             = DenseVector(orderedObjectIds:_*)
      tempVec                  = mean(tempMatrix(::,*)).t
      meanThreshold            = mean(tempVec)
      stdDev                   = stddev(tempVec)
      _                        <- ctx.logger.debug(s"MEAN $meanThreshold")
      thresholdTemperatureMask = tempVec >:> meanThreshold
      hotData                  = objectIdsVec(thresholdTemperatureMask).toArray.toList
      hotData_                 = orderedDumbObjects.filter(x=>hotData.contains(x.objectId))
    } yield hotData_
  }

  def nextNumberOfAccess(nfs:List[Int]): Double ={
    val avgRateOfOfGrowthDecay = averageRateOfGrowthDecay(nfs=nfs)
    nfs.lastOption match {
      case Some(value) => math.floor(value*(avgRateOfOfGrowthDecay+1))
      case None => 0.0
    }
  }
  def averageRateOfGrowthDecay(nfs:List[Int]): Double ={
    val rss = calculateRateOfGrowthDecay(nfs)
    rss match {
      case Some(rsV) =>
        if (rsV.isEmpty || (nfs.length-1) ==0) 0.0
        else rsV.sum / (nfs.length -1)
      case None => 0.0
    }
  }
  def calculateRateOfGrowthDecay(nfs:List[Int]) = {
    val L = nfs.length
    if( L >= 2){
      val x = nfs.zipWithIndex.map{
        case (nf,index)=>
            if(index == L-1) 0
            else (nfs(index+1).toDouble/nf.toDouble)-1
      }
      x.some
    } else Option.empty[List[Double]]

//    val objectIdEvents = Events.on
  }

  def runEffects(events:List[EventX],
                 dumbObject: DumbObject,
                 selectedNode:NodeX,
                 pullNodeX:NodeX,
                 operationId:String = ""
                )(implicit ctx:NodeContext) = for {
//    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    _          <- IO.unit
    objectId   = dumbObject.objectId
    objectSize = dumbObject.objectSize
//    pullFrom   = pullNodeX.httpUrl+s"/api/v6/download/$objectId"
    pullFrom   = if(ctx.config.returnHostname) s"http://${pullNodeX.nodeId}:6666/api/v6/download/$objectId" else pullNodeX.httpUrl+s"/api/v6/download/$objectId"
    unsafeUri  = if(ctx.config.returnHostname)s"http://${selectedNode.nodeId}:6666/pull" else s"${selectedNode.httpUrl+"/pull"}"
    //
    req      = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(unsafeUri),
      headers = Headers(
        Header.Raw(CIString("Pull-From"),pullFrom),
        Header.Raw(CIString("Operation-Id"),operationId)
      )
    )
//    pullResponse <- client.toHttpApp.run(req)
    _ <- ctx.client.stream(req).evalMap{ pullResponse => for {

      _ <- ctx.logger.debug("PULL_STATUS "+pullResponse.status.toString())
      _ <- if(pullResponse.status.code != 200) ctx.errorLogger.error(s"PULL $objectId ${selectedNode.nodeId} ${pullResponse.status.code}")
      else {
        for {
          _ <- IO.unit
          pullHeaders  = pullResponse.headers
          //
          downloadSt             = pullHeaders.get(CIString("Download-Service-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
          uploadSt               = pullHeaders.get(CIString("Upload-Service-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
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
                nodeId = ctx.config.nodeId,
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
          balanceTemp = false
          balanceTemperatureEvents <- if(balanceTemp) {
            for {
              _ <- IO.unit
              downloads        = Events.getDownloadsByObjectId(objectId = objectId,events = events)
              dowloadsCounter  = downloads.length
              replicaNodesIds  = Events.getReplicaNodeIdsByObjectId(getsEvents = downloads)
              totalOfReplicas  = replicaNodesIds.length
              downloadsPerNode = math.ceil((dowloadsCounter/(totalOfReplicas+1).toDouble))
//              _                <- ctx.logger.debug(s"REPLICAS $totalOfReplicas")
//              _                <- ctx.logger.debug(s"DOWNLOADS_TOTAL $dowloadsCounter")
//              _                <- ctx.logger.debug(s"DOWNLOADS_PER_NODE $downloadsPerNode")
              tempEvents       = balanceTemperature(objectId,replicaNodesIds,downloadsPerNode.toInt,pullEndAt+300)
            } yield tempEvents
          }
          else List.empty[EventX].pure[IO]
          //
          selectedNodeId     = selectedNode.nodeId
//          _                  <- ctx.logger.info(s"UPLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
//          _                  <- ctx.logger.info(s"DOWNLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
          _ <- Events.saveEvents(
            events = List(
              Replicated(
                serialNumber     = 0,
                nodeId           = selectedNode.nodeId,
                objectId         = objectId,
                objectSize       = objectSize,
                replicas         = List(selectedNode.nodeId),
                rf               = 1,
                timestamp        = pullEndAt,
                serviceTimeNanos = replicationServiceTimeNano
              ),
              Put(
                serialNumber = 0,
                objectId = objectId,
                objectSize = objectSize,
                nodeId = selectedNodeId,
                timestamp = pullEndAt+200,
                serviceTimeNanos = uploadSt,
                userId  = "DAEMON",
                correlationId=operationId
              ),
//              Get(
//                serialNumber     = 0,
//                objectId         = objectId,
//                objectSize       = objectSize,
//                nodeId           = pullNodeX.nodeId,
//                timestamp        = pullEndAt+300,
//                serviceTimeNanos = downloadSt,
//                userId            = "SYSTEM_REP_DAEMON",
//                correlationId=operationId
//              ),
            ) ++ evictedEvents ++ balanceTemperatureEvents
          )

        } yield ()
      }
    } yield ()

    }.compile.drain
//      .start

//    _ <- finalizer
  } yield()

}
