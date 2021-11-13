package mx.cinvestav.events

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{Del, UpdatedNodePort}

import java.util.UUID
import scala.util.Random
//
import breeze.linalg._
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{Get, Put,
  TransferredTemperature => SetDownloads,
  Evicted,
  Replicated,
  AddedNode,
  RemovedNode,
  Missed,
  EventX,
  EventXOps,
  Push,
  Pull=>PullEvent
}
//import mx.cinvestav.commons.events.{AddedNode,EventX, Evicted, Missed, RemovedNode, Replicated,EventXOps}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.commons.balancer.v3.UF
//
import scala.collection.immutable.ListMap
//import mx.

object Events {
//  case class


//

  case class GetInProgress(
                            serialNumber: Long,
                            nodeId: String,
                            timestamp: Long,
                            monotonicTimestamp: Long,
                            objectId:String,
                            objectSize:Long,
                            correlationId: String,
                            eventType: String ="GET_IN_PROGRESS",
                            eventId: String= UUID.randomUUID().toString,
                            serviceTimeNanos: Long=0L,
                         ) extends EventX
//
  case class DumbObject(objectId:String,objectSize:Long)
//

  def getAndPutsByNode(events:List[EventX]): Map[String, List[EventX]] = {
    val getsAndPuts:List[EventX] = events.filter{
      case _:Get | _:Put => true
      case _             => false
    }
    getsAndPuts.groupBy(_.nodeId)
  }


  def getAvgServiceTimeByNode(events:List[EventX]): Map[String, Double] = {
    getAndPutsByNode(events=events)
      .map{
        case (nodeId, operations) =>
          nodeId -> avgServiceTime(operations)
      }
  }
  def avgServiceTime(events:List[EventX]) = (events.map(_.serviceTimeNanos).sum.toDouble/1000000000)/events.length.toDouble

  def avgArrivalRate(events:List[EventX], startAt:Long, endAt:Long) = {
    val _events    = events.filter(x=>x.monotonicTimestamp > startAt && x.monotonicTimestamp < endAt)
    val N          = _events.length.toDouble
    if(N == 0  ) 0.0
    else if ( N == 1) N
    else {
      val totalTime  = (endAt - startAt).toDouble
      N / totalTime
    }
//    val firstEvent = events.

  }
//  def

//

  def calculateMemoryUFByNode(events:List[EventX],objectSize:Long): Map[String, Double] = {
    val nodes = getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
    val initNodesUFs = nodes.map{
      case (nodeId, _) => nodeId->0.0
    }
   initNodesUFs |+| events.filter{
      case _:Get | _:GetInProgress => true
      case _ => false
    }.groupBy(_.nodeId).map{
      case (nodeId, events) =>  nodeId ->
        events.groupBy(_.correlationId)
          .map{
            case (_, es) if es.length==1  => es.map{
              case x:GetInProgress => x.objectSize
              case  _ => 0
            }.sum
            case (_,_) => 0
          }.sum
    }.map{
      case (nodeId, usedMemory) =>
        val maybeNode = nodes.get(nodeId)
        maybeNode match {
          case Some(node) => node.nodeId -> UF.calculate(total = node.totalMemoryCapacity,used = objectSize+usedMemory ,objectSize = objectSize)
          case None => ""->1.0
        }
    }
      .filter(_._1.nonEmpty)
  }

  def getNodeById(nodeId:String,events:List[EventX]): Option[NodeX] = {
    getAllNodeXs(events=events).find(_.nodeId == nodeId)
  }
  def getNodesByIsd(nodeIds:List[String],events:List[EventX]): List[NodeX] =
    getAllNodeXs(events=events).filter(x=>nodeIds.contains(x.nodeId))

  //
//
 def getReplicaCounter (guid:String,events:List[EventX],arMap:Map[String,NodeX]) = {
  val definedAt = Events.getHitCounterByNode(events = events).filter{
    case (nodeId, value) => value.contains(guid) && arMap.contains(nodeId)
  }
  val defaultNodesReplicaCounter = arMap.map{
    case (nodeId, _) =>  nodeId -> 0
  }
  val nodeWithReplicaCounter = defaultNodesReplicaCounter |+| definedAt.map{
    case (nodeId, value) => nodeId -> value.getOrElse(guid,0)
  }
  nodeWithReplicaCounter
}
  def balanceByReplica(downloadBalancer:String="ROUND_ROBIN",objectSize:Long=0)(guid:String,arMap:Map[String,NodeX],events:List[EventX])(implicit ctx:NodeContext):Option[NodeX] = {
    downloadBalancer match {
      case "LEAST_CONNECTIONS" =>
        val nodeWithReplicaCounter = getReplicaCounter(guid,events,arMap)
        nodeWithReplicaCounter.minByOption(_._2).flatMap{
          case (nodeId, _) => arMap.get(nodeId)
        }
      case "ROUND_ROBIN" =>
        val nodeWithReplicaCounter = getReplicaCounter(guid,events,arMap)
        val totalOfReqs = nodeWithReplicaCounter.values.sum
        val index = totalOfReqs % nodeWithReplicaCounter.size
        val nodeId = nodeWithReplicaCounter.toList.get(index).map(_._1).get
        arMap.get(nodeId)
//      ESTO ES UNA MAMADA
      case "UF" =>
        val ufs = Events.calculateMemoryUFByNode(events=events,objectSize = objectSize).filter{
          case (nodeId, _) => arMap.contains(nodeId)
        }
        val minUF = ufs.minBy(_._2)._1
        arMap.get(minUF)
      case "PSEUDO_RANDOM" =>
        val nodeIds =  arMap.keys.toList
        val randomIndex = new Random().nextInt(arMap.size)
        val randomNodeId = nodeIds(randomIndex)
        arMap.get(randomNodeId)
      case _ =>
        val nodeWithReplicaCounter = getReplicaCounter(guid,events,arMap)
        nodeWithReplicaCounter.minByOption(_._2).flatMap{
          case (nodeId, _) => arMap.get(nodeId)
        }
    }


  }

  //

  def sequentialMonotonic(lastSerialNumber:Int,events:List[EventX]): IO[List[EventX]] = for {
    transformeEvents <- events.zipWithIndex.traverse{
      case (event,index)=>
        for {
          now      <- IO.monotonic.map(_.toNanos)
          newEvent = event match {
            case x:AddedNode => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:RemovedNode => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Put => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Get => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Del => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Evicted => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Replicated => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Push => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:PullEvent => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:Missed => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:SetDownloads => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:GetInProgress => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case _ => event
          }
        } yield newEvent
    }
  } yield transformeEvents

  def saveEvents(events:List[EventX])(implicit ctx:NodeContext) = for {
    currentState     <- ctx.state.get
    currentEvents    = currentState.events
    lastSerialNumber = currentEvents.length
    transformeEvents <- sequentialMonotonic(lastSerialNumber,events=events)
    _                <- ctx.state.update{ s=>
      s.copy(events =  s.events ++ transformeEvents )
    }
  } yield()

//
  def getObjectSize(objectId:String,events:List[EventX]) =
    onlyPutos(events = events)
      .map(_.asInstanceOf[Put])
      .find(_.objectId === objectId)
      .map(_.objectSize)
//

  def getUserIds(events:List[EventX]): List[String] =
    onlyPutos(events=events)
    .map(_.asInstanceOf[Put])
    .map(_.userId).distinct

  def getObjectsByUserId(userId:String,events:List[EventX]): List[DumbObject] =
    onlyPutos(events = events)
      .map(_.asInstanceOf[Put])
      .filter(_.userId == userId)
      .map(x=>DumbObject(x.objectId,x.objectSize))
      .distinctBy(_.objectId)

  def getObjectById(objectId:String,events:List[EventX]):Option[DumbObject] = onlyPutos(events=events).map(_.asInstanceOf[Put]).find(_.objectId == objectId)
    .map(x=>DumbObject(x.objectId,x.objectSize))
  //      .distinctBy(_.objectId)
//
  def getVolumen(events:List[EventX]) = {
    val hitCounter = getHitCounterByNode(events = events).values.foldLeft(Map.empty[String,Int])(_ |+| _)
    val users      = getUserIds(events =events)
    val userAndObjects = users.map{ userId =>
      userId -> getObjectsByUserId(userId=userId,events=events)
    }
    println(hitCounter)
  }
//
  def calculateUFs(events:List[EventX],objectSize:Long=0): Map[String, Double] = {
    getAllNodeXs(events=events)
      .map(x=>(
        x.nodeId, UF
        .calculate(x.totalStorageCapacity,x.usedStorageCapacity,objectSize = objectSize) )
      )
      .toMap
  }
//
  def getOperationsByNodeId(nodeId:String,events:List[EventX]): List[EventX] = events.filter{
    case d:Get => d.nodeId == nodeId
    case u:Put => u.nodeId == nodeId
    case _ => false
  }

  def onlyPutos(events:List[EventX]): List[EventX] = events.filter{
      case up:Put => true
      case _ => false
    }
  def onlyGets(events:List[EventX]): List[EventX] = events.filter{
    case up:Get => true
    case _ => false
  }

  def onlyAddedNode(events:List[EventX]): List[EventX] = events.filter{
    case _:AddedNode => true
    case _ => false
  }
  def getNodeIds(events: List[EventX]) = onlyAddedNode(events = events).map(_.asInstanceOf[AddedNode]).map(_.addedNodeId).sorted
//
  def getHitCounterByNode(events:List[EventX]): Map[String, Map[String, Int]] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> (x -> 0)).toMap
    Events.onlyGets(events = events).map(_.asInstanceOf[Get]).map{ e=>
      Map(e.nodeId -> Map(e.objectId -> 1)  )
    }
      .foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
      .map{
        case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
      }
  }

  def getHitCounterByNodeV2(events:List[EventX]): Map[String, Map[String, Int]] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
   val filtered =  events.filter{
      case _:Get | _:SetDownloads => true
      case _ => false
    }.foldLeft(List.empty[EventX]){
     case (_events,currentEvent)=> currentEvent match {
       case st:SetDownloads => _events.filterNot{
         case d:Get =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
         case d:SetDownloads =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
         case _=> false
       } :+ st
       case d:Get => _events :+ d
     }
   }.map{
     case d:Get => Map(d.nodeId -> Map(d.objectId -> 1)  )
     case st: SetDownloads => Map(st.nodeId -> Map(st.objectId -> st.counter)  )
   }.foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
     .map{
       case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
     }
    filtered.toList.sortBy(_._1).toMap
  }

  def getGlobalHitRatio(events:List[EventX]): Map[String, Map[String, Double]] = {
     events.filter{
      case _:Get | _:Missed => true
      case _ => false
    }.map{
      case d:Get =>
        Map(d.nodeId->
          Map("hits"-> 1.0 , "miss" -> 0.0 ,"hitRatio"-> 0.0)
        )
      case m:Missed =>
        Map(
          m.nodeId -> Map("hits"-> 0.0 , "miss" -> 1.0 ,"hitRatio"->0.0)
        )
    }.foldLeft(Map.empty[String,Map[String,Double]  ])(_ |+| _)
      .map{
        case (nodeId, info) =>
          val misses = info.get("miss")
          val hits = info.get("hits")
          nodeId -> info.updatedWith("hitRatio")( op =>
            op.flatMap(_ =>  for {
              hit <- hits
              miss <- misses
            } yield hit/(hit+miss)
            )
          )
      }
  }

  def getHitInfoByNode(events:List[EventX]) = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> (x -> 0)).toMap

//    Events.onlyDownloaded(events = events)
      events.filter{
        case _:Get | _:Missed  =>true
        case _ => false
      }.map{
        case d:Get =>
          Map(
            d.nodeId -> Map(
              d.objectId -> Map("miss"->0.0,"hits"->1.0,"hitRatio"->0.0)
            )
          )
        case d:Missed =>
          Map(
            d.nodeId -> Map(
              d.objectId -> Map("miss"->1.0,"hits"->0.0,"hitRatio"->0.0)
            )
          )
      }
        .foldLeft(Map.empty[String,
          Map[String,Map[String,Double]]
        ])( _ |+| _)
        .map{
          case (nodeId,objects) =>
            nodeId ->  objects.map{
              case (objectId,infoObject)  =>
                val miss = infoObject.get("miss")
                val hits = infoObject.get("hits")
                objectId -> infoObject.updatedWith("hitRatio")( op =>
                  op.flatMap(_=>
                    hits.mproduct(hit=>miss.map(misses=> hit / (misses + hit)))._2F
                  )
                )
            }
        }
  }

  def filterNodesByHitRatio(hitRatio:Double,events:List[EventX]): Map[String, Map[String, Double]] = {
    getGlobalHitRatio(events = events).filter{
      case (nodeId, info) => info.getOrElse("hitRatio",0.0) >= hitRatio
    }
  }

  def generateMatrix(events:List[EventX]): DenseMatrix[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNode(events = events).toList.sortBy(_._1):_*
    ).map{
      case (nodeId, counter) => nodeId-> ListMap(counter.toList.sortBy(_._1):_*)
    }
//      .toMap
//    println(s"HIT_COUNTER ${hitCounter}")
    val vectors = hitCounter
  .toList.sortBy(_._1)
  .map{
      case (nodeId, objectDownloadCounter) =>
        val values = objectDownloadCounter.values.toArray.map(_.toDouble)
        val vec = DenseVector(values:_*)
        vec
    }.toList
    DenseMatrix(vectors.map(_.toArray):_*)
  }

  def generateMatrixV2(events:List[EventX]): DenseMatrix[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNodeV2(events = events).toList.sortBy(_._1):_*
    ).map{
      case (nodeId, counter) => nodeId-> ListMap(counter.toList.sortBy(_._1):_*)
    }
    //      .toMap
    //    println(s"HIT_COUNTER ${hitCounter}")
    val vectors = hitCounter
      .toList.sortBy(_._1)
      .map{
        case (nodeId, objectDownloadCounter) =>
          val values = objectDownloadCounter.values.toArray.map(_.toDouble)
          val vec = DenseVector(values:_*)
          vec
      }
    DenseMatrix(vectors.map(_.toArray):_*)
  }

  def generateTemperatureMatrix(events:List[EventX]): DenseMatrix[Double] =  {
    val x = Events.generateMatrix(events=events)
    val dividend = sum(x(*,::))
    x(::,*)  / dividend
  }
  def generateTemperatureMatrixV2(events:List[EventX]): DenseMatrix[Double] =  {
    val x = Events.generateMatrixV2(events=events)
    val dividend = sum(x(*,::))
    x(::,*)  / dividend
  }

  def getObjectIds(events:List[EventX]): List[String] = Events.onlyPutos(events = events).map(_.asInstanceOf[Put]).map(_.objectId).distinct


  def generateDistributionSchema(events:List[EventX]): Map[String, List[String]] =
    Events.onlyPutos(events = events).map(_.asInstanceOf[Put]).map{ up=>
      Map(up.objectId -> List(up.nodeId))
    }.foldLeft(Map.empty[String,List[String]])(_ |+| _)

  def getDownloadsByObjectId(objectId:String,events:List[EventX])    =
    onlyGets(events = events).map(_.asInstanceOf[Get]).filter(_.objectId == objectId)
  def getReplicaNodeIdsByObjectId(getsEvents:List[Get]) = getsEvents.map{ x=>
    x.nodeId
  }.distinct



  def getAllUpdatedNodePort(events:List[EventX]) = {
    events.filter{
      case _:UpdatedNodePort=>true
      case _ => false
    }
  }
  def getAllNodeXs(events: List[EventX]): List[NodeX] = {
//    val nodesWithObjectSizes = Events.nodesToObjectSizes(events)
//    val updatedNodePorts = getAllUpdatedNodePort(events =events).map(_.asInstanceOf[UpdatedNodePort])
//    val updatedNodePortMap = updatedNodePorts.map(x=>x.nodeId->x.newPort).toMap
    val nodesAndDumbObjects   = Events.nodesToDumbObject(events)
    onlyAddedNode(events).map(_.asInstanceOf[AddedNode])
      .map{ an =>
        an.addedNodeId -> NodeX(nodeId = an.addedNodeId,ip=an.ipAddress,port=an.port,
          totalStorageCapacity = an.totalStorageCapacity,
          availableStorageCapacity = an.totalStorageCapacity,
          usedStorageCapacity = 0L,
          cacheSize = an.cacheSize,
          usedCacheSize = 0,
          availableCacheSize = an.cacheSize,
          cachePolicy = an.cachePolicy,
          metadata = Map.empty[String,String]
        )
      }.toMap.map{
      case (nodeId, node) =>
//        val element = nodesWithObjectSizes.getOrElse(nodeId,List.empty[Long])
        val element = nodesAndDumbObjects.getOrElse(nodeId,List.empty[DumbObject])
//          .take(node.cacheSize)
//        val objects = nodesAndDumbObjects
//        val usedStorageCapacity   = element.sum
        val usedStorageCapacity = nodesAndDumbObjects.getOrElse(nodeId,List.empty[DumbObject]).map(_.objectSize).sum
//  .take(node.cacheSize)
        val availableStorageCapacity = node.totalStorageCapacity - usedStorageCapacity
//        val totalOfOps = getOperationsByNodeId(nodeId = nodeId,events = events).length
        val totalOfOpsv2 = getOperationsByNodeId(nodeId = nodeId, events = events).count {
          case _: Get => true
          case _ => false
        }
        val totalOfOpsv3 = getOperationsByNodeId(nodeId = nodeId, events = events).count {
          case _: Put => true
          case _ => false
        }
        node.copy(
          usedStorageCapacity = usedStorageCapacity,
          availableStorageCapacity = availableStorageCapacity,
          usedCacheSize = element.length,
          availableCacheSize = node.cacheSize - element.length,
          metadata = Map(
            "TOTAL_REQUESTS"-> (totalOfOpsv2+totalOfOpsv3).toString,
            "DOWNLOAD_REQUESTS"-> totalOfOpsv2.toString,
            "UPLOAD_REQUESTS"-> totalOfOpsv3.toString,
          ),
//          port =
//            updatedNodePortMap.getOrElse(node.nodeId, node.port)
        )
    }
  }.toList
  def nodesToObjectSizes(events:List[EventX]): Map[String, List[Long]] =
    onlyPutos(events).map(_.asInstanceOf[Put]).map{ up =>
      Map(up.nodeId -> List(up.objectSize))
    }.foldLeft(Map.empty[String,List[Long]])(_ |+| _)


  def nodesToObjectIds(events:List[EventX]): Map[String, List[String]] =
    onlyPutos(events).map(_.asInstanceOf[Put]).map{ up =>
    Map(up.nodeId -> List(up.objectId))
  }.foldLeft(Map.empty[String,List[String]])(_|+|_)

  def nodesToDumbObject(events:List[EventX]): Map[String, List[DumbObject]] =
    onlyPutos(events).map(_.asInstanceOf[Put]).map{ up =>
      Map(up.nodeId -> List( DumbObject(up.objectId,up.objectSize)  ))
    }.foldLeft(Map.empty[String,List[DumbObject]])(_|+|_)

  def orderAndFilterEvents(events:List[EventX]):List[EventX] =
    Events.filterEvents(EventXOps.OrderOps.byTimestamp(events=events).reverse)

  def orderAndFilterEventsMonotonic(events:List[EventX]):List[EventX] =
    filterEventsMonotonic(events.sortBy(_.monotonicTimestamp))
  def orderAndFilterEventsMonotonicV2(events:List[EventX]):List[EventX] =
    filterEventsMonotonicV2(events.sortBy(_.monotonicTimestamp))
//    Events.filterEvents(EventXOps.OrderOps.byTimestamp(events=events).reverse)


  def filterEventsMonotonicV2(events:List[EventX]): List[EventX] = {
    val newEvents = collection.mutable.ListBuffer.empty[EventX]
    events.foreach {
      case ev: Evicted =>
        newEvents.filterNot {
          case get: Get => get.monotonicTimestamp < ev.monotonicTimestamp && get.objectId == ev.objectId && (get.nodeId == ev.fromNodeId || get.nodeId == ev.nodeId)
          case put: Put => put.monotonicTimestamp < ev.monotonicTimestamp && put.objectId == ev.objectId && (put.nodeId == ev.fromNodeId || put.nodeId == ev.nodeId)
          case rep: Replicated => rep.monotonicTimestamp < ev.monotonicTimestamp && rep.objectId == ev.objectId && (rep.nodeId == ev.fromNodeId || rep.nodeId == ev.nodeId)
          case _ => false
        }
      case rn: RemovedNode => newEvents.filterNot {
        case an: AddedNode => an.monotonicTimestamp < rn.monotonicTimestamp && an.addedNodeId == rn.removedNodeId
        case an: Put => an.monotonicTimestamp < rn.monotonicTimestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case an: Get => an.monotonicTimestamp < rn.monotonicTimestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case _ => false
      }
      case e => newEvents.append(e)
    }
    newEvents.toList
  }

  def filterEventsMonotonic(events:List[EventX]): List[EventX] = events.foldLeft(List.empty[EventX]){
    case ((events,e))=> e match {
      case ev:Evicted =>
        events.filterNot{
        case get:Get => get.monotonicTimestamp < ev.monotonicTimestamp && get.objectId == ev.objectId && (get.nodeId == ev.fromNodeId || get.nodeId == ev.nodeId)
        case put:Put => put.monotonicTimestamp < ev.monotonicTimestamp && put.objectId == ev.objectId && (put.nodeId == ev.fromNodeId || put.nodeId == ev.nodeId)
        case rep:Replicated => rep.monotonicTimestamp < ev.monotonicTimestamp && rep.objectId == ev.objectId && (rep.nodeId == ev.fromNodeId || rep.nodeId == ev.nodeId)
        case _=> false
      }
      case rn:RemovedNode => events.filterNot {
        case an: AddedNode => an.monotonicTimestamp < rn.monotonicTimestamp && an.addedNodeId == rn.removedNodeId
        case an: Put=> an.monotonicTimestamp < rn.monotonicTimestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case an: Get => an.monotonicTimestamp < rn.monotonicTimestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case _ => false
      }
      case _ => events :+ e
    }
  }

  def filterEvents(events:List[EventX]): List[EventX] = events.foldLeft(List.empty[EventX]){
    case ((events,e))=> e match {
      case _:AddedNode | _:Put | _:Get | _:Replicated | _:Missed | _:SetDownloads => events :+ e
      case ev:Evicted => events.filterNot{
        case get:Get => get.timestamp < ev.timestamp && get.objectId == ev.objectId && (get.nodeId == ev.fromNodeId || get.nodeId == ev.nodeId)
        case put:Put => put.timestamp < ev.timestamp && put.objectId == ev.objectId && (put.nodeId == ev.fromNodeId || put.nodeId == ev.nodeId)
        case rep:Replicated => rep.timestamp < ev.timestamp && rep.objectId == ev.objectId && (rep.nodeId == ev.fromNodeId || rep.nodeId == ev.nodeId)
        case _=> false
      }
      case rn:RemovedNode => events.filterNot {
        case an: AddedNode => an.timestamp < rn.timestamp && an.addedNodeId == rn.removedNodeId
        case an: Put=> an.timestamp < rn.timestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case an: Get => an.timestamp < rn.timestamp && an.nodeId == rn.removedNodeId && an.nodeId == rn.removedNodeId
        case _ => false
      }
    }
  }

}
