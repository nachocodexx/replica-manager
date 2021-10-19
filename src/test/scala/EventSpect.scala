import breeze.linalg._
import breeze.numerics._
import mx.cinvestav.commons.events.SetDownloads
//import breeze.linalg.di
import breeze.stats.mean
import mx.cinvestav.commons.events.{Missed, Put}

import java.util.UUID
//{DenseMatrix, DenseVector, sum}
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.eventXEncoder
import mx.cinvestav.commons.events
import mx.cinvestav.commons.events.{Get,AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, RemovedNode, Uploaded,Pull=>PullEvent}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
class EventSpect extends munit .CatsEffectSuite {

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

  test("Demo events"){
    val initTimestamp = 1633963638000L
    val baseMissed = Missed(
      eventId = UUID.randomUUID().toString,
      serialNumber = 0,
      nodeId = "cache-0",
      objectId = "F0",
      objectSize = 1000,
      timestamp = 0L,
      milliSeconds = 0L
    )
    val basedUploaded = Uploaded(
        eventId = "",
        serialNumber = 0,
        nodeId = "lb-0",
        objectId = "F0",
        objectSize = 100,
        selectedNodeId = "cache-0",
        timestamp = 0L,
        milliSeconds = 0L
      )
    val baseDownloaded = Downloaded(
        eventId = "event-",
        serialNumber = 0,
        objectId = "F0",
        objectSize = 100,
        nodeId="lb-0",
        selectedNodeId = "cache-0",
        timestamp = 0L,
        milliSeconds = 0L
      )
    val baseAddedNode = AddedNode(
        eventId = "",
        serialNumber = 0,
        addedNodeId = "cache-0",
        ipAddress = "10.0.0.2",
        nodeId="lb-0",
        port=4000,
        totalStorageCapacity = 1000,
        cacheSize = 10,
        cachePolicy = "LRU",
        timestamp = 0L,
        milliSeconds = 0L
      )
    val baseSetDownloads = SetDownloads(
      eventId = "",
      serialNumber = 0,
      nodeId = "cache-0",
      objectId = "F1",
      counter = 0,
      timestamp = 0,
//      eventType = ???,
      milliSeconds = 0
    )
//
    val rawEvents = List(
//    cache-0 ADDED
      baseAddedNode,
//    cache-1 ADDED
      baseAddedNode.copy(addedNodeId = "cache-1"),
//    ______________________________________
      baseAddedNode.copy(addedNodeId = "cache-2"),
      //    ______________________________________
//    F0 UPLOADED to cache-0
      basedUploaded,
//    F0 DOWNLOADED from cache-0
      baseDownloaded,
      baseDownloaded,
      baseDownloaded,
      baseDownloaded,
      basedUploaded.copy(objectId = "F3"),
      baseDownloaded.copy(objectId = "F3"),
//    ______________________________________
//    F1 UPLOADED to cache-1
      basedUploaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
//    (F1 DOWNLOADED from cache-1) x 10
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
//      basedUploaded.copy(selectedNodeId = "cache-2",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
//
      baseSetDownloads.copy(nodeId = "cache-1",objectId = "F1",counter = 10),
      baseDownloaded.copy(selectedNodeId = "cache-1",objectId = "F1"),
//
      baseSetDownloads.copy(nodeId = "cache-2",objectId = "F1",counter = 10),
      baseDownloaded.copy(selectedNodeId = "cache-2",objectId = "F1"),
//    ______________________________________
      basedUploaded.copy(selectedNodeId = "cache-2",objectId = "F2"),
      baseDownloaded.copy(selectedNodeId = "cache-2",objectId = "F2"),
      baseDownloaded.copy(selectedNodeId = "cache-2",objectId = "F2"),
      //      basedUploaded.copy(selectedNodeId = "cache-0",objectId = "F2"),
      //      baseDownloaded.copy(selectedNodeId = "cache-0",objectId = "F2"),
//      baseMissed,
//      baseMissed,
//      baseMissed,
//      baseMissed,
//      baseMissed,
////
//      baseMissed.copy(nodeId = "cache-1"),

    )
    val events = Events.filterEvents(
      preProcessingEvents(events = rawEvents,initTimestamp = initTimestamp)
    )
//    val x = Events.getHitCounterByNodeV2(events = events)
//    println(x.asJson)
    val orderedObjectIds = Events.getObjectIds(events=events).sorted
    val orderedNodeIds = Events.getNodeIds(events=events)
    val tempMatrix = Events.generateTemperatureMatrixV2(events=events)
//    val i = tempMatrix.linearIndex(0,1)
    val x= argmax(tempMatrix(*,::)).toArray.toList.zip(orderedNodeIds.zipWithIndex.map(_._2))
    println(x)
//      .toDenseVector
    println(orderedObjectIds)
    println(orderedNodeIds)
//    println(i)
    println(tempMatrix)
//    println(x,i)
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
//        selectedNodeId = "cache-0",
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
//        selectedNodeId = "cache-0",
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
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "FX",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-4",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F0",
//        objectSize = 100,
//        selectedNodeId = "cache-0",
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
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F2",
//        objectSize = 100,
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      )
//    ) ++ tenDownloadsForF0 ++ List(
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F2",
//        objectSize = 100,
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F2",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-0",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F3",
//        objectSize = 100,
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F4",
//        objectSize = 100,
//        selectedNodeId = "cache-1",
//        timestamp = 1633963680000L
//      ),
//      Uploaded(
//        eventId = "event-5",
//        serialNumber = 4,
//        nodeId = "lb-0",
//        objectId = "F5",
//        objectSize = 100,
//        selectedNodeId = "cache-2",
//        timestamp = 1633963680000L
//      ),
//      Downloaded(
//        eventId = "event-",
//        serialNumber = 0,
//        objectId = "F5",
//        objectSize = 100,
//        nodeId="lb-0",
//        selectedNodeId = "cache-2",
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
////      Map(e.selectedNodeId -> Map(e.objectId -> 1)  )
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
