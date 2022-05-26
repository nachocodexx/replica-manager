import cats.implicits._
import breeze.linalg._
import breeze.numerics._
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Main
import mx.cinvestav.commons.events.{GetCompleted, Push, PutCompleted, Replicated}
import mx.cinvestav.events.Events.GetInProgress
import mx.cinvestav.replication.DataReplication
import org.http4s.blaze.client.{BlazeClient, BlazeClientBuilder}

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.Duration
//import breeze.linalg.di
import breeze.stats.{mean,stddev}
import mx.cinvestav.commons.events.{Missed, Put}

import java.util.UUID
//{DenseMatrix, DenseVector, sum}
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.commons.events.{Get,AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, RemovedNode, Uploaded,Pull=>PullEvent,TransferredTemperature => SetDownloads}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import concurrent.duration._
import language.postfixOps
class EventSpect extends munit .CatsEffectSuite {

  override def munitTimeout: Duration = Int.MaxValue seconds

  val baseEvicted = Evicted(
    eventId = UUID.randomUUID().toString,
    serialNumber = 0,
    nodeId = "cache-0",
    objectId = "F0",
    objectSize = 1000,
    timestamp = 0L,
    serviceTimeNanos = 0L,
    fromNodeId = ""
  )
  val baseMissed = Missed(
    eventId = UUID.randomUUID().toString,
    serialNumber = 0,
    nodeId = "cache-0",
    objectId = "F0",
    objectSize = 1000,
    timestamp = 0L,
    serviceTimeNanos = 0L
  )
  val basedUploaded = Put(
    objectId         = "de42b21f-e0d8-43dd-abb9-75660ffc33d1",
    objectSize       = 100,
    nodeId           = "cache-0",
    timestamp        = 0L,
    serviceTimeNanos = 0L,
    serialNumber     = 0,
    correlationId    =  "op-0"
  )

  val baseDownloadedInProgress = GetInProgress(
    eventId = "event-",
    serialNumber = 0,
    objectId = "F0",
    objectSize = 100,
    nodeId = "cache-0",
    timestamp = 0L,
    monotonicTimestamp = 0L,
    correlationId = ""
  )
  val baseDownloaded = Get(
    eventId = "event-",
    serialNumber = 0,
//    objectId = "F0",
    objectId = "de42b21f-e0d8-43dd-abb9-75660ffc33d1",
    objectSize = 100,
    //        nodeId="lb-0",
    nodeId = "cache-0",
    timestamp = 0L,
    serviceTimeNanos = 0L,
  )
  val baseAddedNode = AddedNode(
    eventId = "",
    serialNumber = 0,
    addedNodeId = "cache-0",
    ipAddress = "10.0.0.2",
    nodeId="lb-0",
    port=4000,
    totalStorageCapacity = 1000,
    timestamp = 0L,
    serviceTimeNanos = 0L
  )
  val baseSetDownloads = SetDownloads(
    eventId = "",
    serialNumber = 0,
    nodeId = "cache-0",
    objectId = "F1",
    counter = 0,
    timestamp = 0,
    //      eventType = ???,
    serviceTimeNanos = 0,
    userId = ""
  )

  val rawEvents = List(
    //    cache-0 ADDED
    baseAddedNode,
    //    cache-1 ADDED
//    baseAddedNode.copy(addedNodeId = "cache-1"),
    //    ______________________________________
//    baseAddedNode.copy(addedNodeId = "cache-2"),
    //    ______________________________________
    //    F0 UPLOADED to cache-0
    basedUploaded,
    //    F0 DOWNLOADED from cache-0
    baseDownloaded,
  )



  def preProcessingEvents(events:List[EventX],initTimestamp:Long):List[EventX]= events.zipWithIndex.map{
    case (e, i) => e match {
      case up:Uploaded => up.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
      case dd:Downloaded => dd.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
      case an:AddedNode => an.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
      case rn:RemovedNode => rn.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
      case ev:Evicted => ev.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
      case put:Put => put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
      case put:Get => put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
      case put:Del => put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
      case put:PullEvent => put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
      case put:Missed => put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
      case put:SetDownloads=> put.copy(eventId = s"event-$i",serialNumber = i,timestamp =initTimestamp+i )
    }
  }
//  P(FAi) =
  test("K"){
    val es = List(
      AddedNode.empty.copy(addedNodeId = "sn-0"),
      AddedNode.empty.copy(addedNodeId = "sn-1"),
      AddedNode.empty.copy(addedNodeId = "sn-2"),
//    ________________________________________________________________
      PutCompleted.empty.copy(nodeId = "sn-0",objectId="f0",monotonicTimestamp = 1),
      PutCompleted.empty.copy(nodeId = "sn-1",objectId="f0",monotonicTimestamp = 1),
      PutCompleted.empty.copy(nodeId = "sn-1",objectId="f1",monotonicTimestamp = 1),
      PutCompleted.empty.copy(nodeId = "sn-2",objectId="f1"),
//    ________________________________________________________________
      GetCompleted.empty.copy(nodeId = "sn-0",objectId = "f0",monotonicTimestamp = 1),
      GetCompleted.empty.copy(nodeId = "sn-0",objectId = "f0",monotonicTimestamp = 1),
      GetCompleted.empty.copy(nodeId = "sn-0",objectId = "f0",monotonicTimestamp = 1),
//    ________________________________________________________________
      GetCompleted.empty.copy(nodeId = "sn-1",objectId = "f0",monotonicTimestamp = 1),
      GetCompleted.empty.copy(nodeId = "sn-1",objectId = "f1",monotonicTimestamp = 1),
//    ________________________________________________________________
      GetCompleted.empty.copy(nodeId = "sn-2",objectId = "f0",monotonicTimestamp = 1),
      GetCompleted.empty.copy(nodeId = "sn-2",objectId = "f1",monotonicTimestamp = 1),
    ).asInstanceOf[List[EventX]]
//    val gcbn = Events.getHitCounterByNodeV2(events = es)

    val xs = Events.generateReplicaUtilizationMap(events=es)
    println(xs)
  }
  test("Main"){
    for {
      (client,finalizer)          <- BlazeClientBuilder[IO](global).resource.allocated
      signal                      <- SignallingRef[IO,Boolean](false)
      implicit0(ctx:NodeContext)  <- Main.initContext(client)
      _                           <- Events.saveEvents(events = rawEvents)(ctx=ctx)
      _                           <- Events.saveEvents(events = basedUploaded.copy(correlationId = "op-0")::Nil )
      currentState                <- ctx.state.get
      events                      = Events.orderAndFilterEventsMonotonicV2(currentState.events)
      _                           <- ctx.logger.debug(events.asJson.toString)
    } yield ()
  }

  test("String hash"){
    val x = Map("a" -> List.empty[String] , "b"-> List("A","B"))
    val y = Map("a"-> List("A"),"c"->List("A"),"b"->List("C","D","E") )
    val z = x |+| y
    println(z)
//    val cache0Id = "cache-0"
//    val cache1Id = "cache-1"
//    val xs = List.empty[Int]
//    println(xs.sum)
//    println(cache0Id.hashCode,cache1Id.hashCode)
  }

  test("K1") {
    val xs = List(1,2,3,4,5,6)
    val ys = xs.scanLeft(0.0){
      case (a,b) =>
        a+b
    }
    println(ys)
  }
  test("Monotonic"){
    val rawEvents2:List[EventX] = List(
      baseAddedNode.copy(timestamp = 0,serviceTimeNanos = 1),
      baseAddedNode.copy(addedNodeId = "cache-1",timestamp = 1,serviceTimeNanos = 1),
      basedUploaded.copy(timestamp = 1),
      basedUploaded.copy(nodeId = "cache-1",timestamp = 2,serviceTimeNanos = 1),
//      baseEvicted.copy(nodeId = "cache-0",timestamp = 3),
      basedUploaded.copy(timestamp = 4,objectId = "F2",serviceTimeNanos = 1),
      baseDownloaded.copy(timestamp = 10,serviceTimeNanos = 1),
    ) ++ List.fill(1000000){
      baseDownloaded.copy(timestamp = 10,serviceTimeNanos = 500)
    }
    for {
      _                <- IO.println("HEREEEEEE")
      lastSerialNumber = 0
      newEvents        <- Events.sequentialMonotonic(lastSerialNumber,events = rawEvents2)
      _                <- IO.println("END_SEQUENTIAL")
      orderedEvents    = Events.orderAndFilterEventsMonotonicV2(events = newEvents)
//      orderedEvents    = Events.orderAndFilterEventsMonotonicV2(events = newEvents)
      _                <- IO.println("END_ORDER")
      x                = Events.avgServiceTime(events = orderedEvents)
      startAt          = orderedEvents.head.monotonicTimestamp
      endAt            = startAt+1000000000
      y                = Events.avgArrivalRate(events = orderedEvents,startAt = startAt,endAt = endAt)
      print            = (x:Any)=>IO.println(x.toString)
      _                <- print(y)
      _                <- print(x)
//      currentObjectSize = 100000000

//      x                 = Events.calculateMemoryUFByNode(events = orderedEvents,objectSize = currentObjectSize)
//      _                 <- IO.println(orderedEvents.asJson)
    } yield ()
  }
  test("Demo events"){
    val initTimestamp = 1633963638000L


//
    val rawEvents2 = List(
      baseAddedNode.copy(timestamp = 0),
      baseAddedNode.copy(addedNodeId = "cache-1",timestamp = 1),
      basedUploaded.copy(timestamp = 1),
      basedUploaded.copy(nodeId = "cache-1",timestamp = 2),
      baseEvicted.copy(nodeId = "cache-0",timestamp = 3)
    )
    val x = Events.orderAndFilterEventsMonotonic(events =rawEvents2)
    println(x)





    val _rawEvents:List[EventX] = EventXOps.OrderOps.byTimestamp(List(
      basedUploaded,
      baseEvicted.copy(fromNodeId = "cache-0",timestamp = 1)
    )).reverse
    val events = Events.filterEvents(
      preProcessingEvents(events = rawEvents,initTimestamp = initTimestamp)
    )
//    println(_rawEvents.asJson)
//    val x = Events.getHitCounterByNodeV2(events = events)
//    println(x.asJson)
    val orderedObjectIds = Events.getObjectIds(events=events).sorted
    val orderedNodeIds   = Events.getNodeIds(events=events)
    val tempMatrix       = Events.generateTemperatureMatrixV2(events=events,0)
    val A                = Events.generateMatrixV2(events = events,windowTime = 0)
    println(A)
    val sumA             = sum(A(::,*))
    println(sumA)
    val maxDownloadedObj = argmax(sumA)
    println(A)
    println(orderedObjectIds)
    println(sumA)
    println(maxDownloadedObj)

//    First approach
//    println(A.toString)
    val tempVec       = mean(tempMatrix(::,*)).t
    val totalTempMean = mean(tempVec)
    val totalTempStd  = stddev(tempVec)

//    _______________
    val f0ObjectSize = Events.getObjectSize(objectId = "F0",events = events)
    val users= Events.getUserIds(events = events)
//    println(f0ObjectSize)
//    println(s"TOTAL_MEAN $totalTempMean")
//    println(s"TOTAL_STDDEV $totalTempStd")
//
//    println(orderedObjectIds)
//    println(orderedNodeIds)
//    println(tempVec)
//    println(tempMatrix)
    //    println(x)
    //    val i = tempMatrix.linearIndex(0,1)
    //    val x = argmax(tempMatrix(*,::)).toArray.toList.zip(orderedNodeIds.zipWithIndex.map(_._2))
    //    println(x)
    //      .toDenseVector
//    println(tempMatrix.asJson)
//    val x = Events.generateMatrix(events=events)
//    val dividend = sum(x(*,::))
//    val res = x(::,*)  / dividend
//    println(x)
//    println(dividend)
//    println(res)
    //    val x =
//      Events.getHitInfoByNode(events = events)
//    val globalHitRatio =
//      Events.getGlobalHitRatio(events = events).filter{
//      case (nodeId, info) => info.getOrElse("hitRatio",0.0) >= .85
//    }

//    println(globalHitRatio.asJson)


//    val nodes = Events.toNodeX(events = events)
////    println(nodes.asJson)
//    val matrix = Events.generateMatrix(events=events)
//    val colSumVector = sum(matrix(::, *)).t
//    val totalDownloads = sum(colSumVector)
//    val heatVector = colSumVector/totalDownloads
//    println(matrix)
//    println(s"COL_SUM_VECTOR $colSumVector")
//    println(s"HEAT_VECTOR = $heatVector")
//    println(s"SUM_HEAT_VECTOR ${sum(heatVector)}")
//    println(matrix.asJson)
//    println(heatVector.asJson)
//    println(events.asJson)
  }

//  test("Basic"){

//    val tenDownloadsForF0 = List.fill(5)(
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F0",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-0",
//        timestamp = 1633963680000L,
//        mil
//      )
//    ).zipWithIndex.map{
//      case (downloaded, i) => downloaded.copy(
//        serialNumber = i+5,
//        eventId = downloaded.eventId.concat(s"${i+5}"),
//        timestamp = downloaded.timestamp+i
//      )
//
//    }
//    val initTimestamp = 1633963638000L
//    val rawEvents:List[EventX] = (List(
//      AddedNode(
//        eventId = "event-0",
//        serialNumber = 0,
//        addedNodeId = "cache-0",
//        ipAddress = "localhost",
//        nodeId="lb-0",
//        port=4000,
//        totalStorageCapacity = 1000,
//        cacheSize = 10,
//        cachePolicy = "LRU",
//        timestamp = 1633963638000L,
//        eventType = "ADDED_NODE"
//      ),
//      Uploaded(
//        eventId = "event-x",
//        serialNumber = 1,
//        nodeId = "lb-0",
//        objectId = "F0",
//        objectSize = 100,
//        nodeId = "cache-0",
//        timestamp = 1633963638000L,
//      ),
//      AddedNode(
//        eventId = "event-1",
//        serialNumber = 1,
//        addedNodeId = "cache-1",
//        ipAddress = "localhost",
//        nodeId="lb-0",
//        port=4000,
//        totalStorageCapacity = 1000,
//        cacheSize = 10,
//        cachePolicy = "LRU",
//        timestamp = 1633963660000L,
//        eventType = "ADDED_NODE"
//      ),
//      RemovedNode(
//        eventId = "event-2",
//        serialNumber = 2,
//        nodeId = "lb-0",
//        removedNodeId = "cache-0",
//        timestamp = 1633963670000L,
//        eventType = "REMOVED_NODE"
//      ),
//      AddedNode(
//        eventId = "event-3",
//        serialNumber = 0,
//        addedNodeId = "cache-0",
//        ipAddress = "localhost",
//        nodeId="lb-0",
//        port=4000,
//        totalStorageCapacity = 1000,
//        cacheSize = 10,
//        cachePolicy = "LRU",
//        timestamp = 1633963671000L,
//        eventType = "ADDED_NODE"
//      ),
//      Uploaded(
//        eventId = "event-4",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "FX",
//        objectSize = 100,
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "FX",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-4",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F0",
//        objectSize = 100,
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Evicted(
//        eventId = "event-4",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "FX",
//        objectSize = 100,
//        fromNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F1",
//        objectSize = 100,
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F2",
//        objectSize = 100,
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      )
//    ) ++ tenDownloadsForF0 ++ List(
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F2",
//        objectSize = 100,
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F3",
//        objectSize = 100,
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F4",
//        objectSize = 100,
//        nodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F5",
//        objectSize = 100,
//        nodeId = "cache-2",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F5",
//        objectSize = 100,
//        nodeId="lb-0",
//        nodeId = "cache-2",
//        timestamp = 1633963680000L
//      ),
//    ))
//
//
//    val processedEvents = rawEvents.zipWithIndex.map{
//      case (e, i) => e match {
//        case up:Uploaded => up.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
//        case dd:Downloaded => dd.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
//        case an:AddedNode => an.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
//        case rn:RemovedNode => rn.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
//        case ev:Evicted => ev.copy(eventId = s"event-$i",serialNumber = i,timestamp = initTimestamp+i)
//      }
//    }
//
//    val events =  Events.filterEvents(processedEvents)
////   _______________________________________________________________________
//    val uploaded             = Events.onlyUploaded(events).map(_.asInstanceOf[Uploaded])
//    val nodesWithObjectIds   = Events.nodesToObjectIds(events)
//    val nodesWithObjectSizes = Events.nodesToObjectSizes(events)
//    //    val distributionSchema   = Events.generateDistributionSchema(events = events)
//
//    val availableResources   = Events.toNodeX(events)
////   DOWNLOAD COUNTER
//    val objectsIds           = Events.onlyUploaded(events = events).map(_.asInstanceOf[Uploaded]).map(_.objectId).distinct
//    val dowloadCounter = Events.getHitCounterByNode(events = events)
////    val downloadObjectInitCounter = objectsIds.map(x=> (x -> 0)).toMap
////    println(downloadObjectInitCounter)
////    val dowloadCounter       = Events.onlyDownloaded(events = events).map(_.asInstanceOf[Downloaded]).map{ e=>
////      Map(e.nodeId -> Map(e.objectId -> 1)  )
////    }.foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
////      .map{
////        case (str, value) => (str,downloadObjectInitCounter|+|value)
////      }
////    println(dowloadCounter)
//
////
//    val vectors = dowloadCounter.map{
//      case (nodeId, objectDownloadCounter) =>
//        val values = objectDownloadCounter.values.toArray.map(_.toDouble)
//        val vec = DenseVector(values:_*)
//        vec
//    }.toList
//
//    val counterMatrix = DenseMatrix(vectors.map(_.toArray):_*)
//
//    val summed = sum(counterMatrix(::, *)).t
//    val totalDownloads = sum(summed)
//    val temperatureVec = summed/totalDownloads
//    println(counterMatrix)
//    println(temperatureVec)
////    val numRows = availableResources.length
////    val numCols = objectsIds.length
////    val matrix  = DenseMatrix.zeros[Double](rows = numRows,cols = numCols)
////    val total  =
////    println(matrix)
////    println(summed)
////    println(temperatureVec)
////    println(s"TOTAL = ${sum(temperatureVec)}")
////    println(meaned)
////    println(objectsIds.asJson)
////    println(availableResources.asJson)
////    println(s"MATRIX [$numRows x $numCols]")
////    println(downloadObjectInitCounter.asJson)
////    println(dowloadCounter.asJson)
//
////    println(distributionSchema)
////    println(filterEvents.asJson)
////    println(nodesWithObjectIds.asJson)
////    println(nodesWithObjectSizes.asJson)
////    println(availableResources.asJson)
////
//  }

}
