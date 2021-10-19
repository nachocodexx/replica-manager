package mx.cinvestav.events

import breeze.linalg._
import mx.cinvestav.commons.events.SetDownloads
//{DenseMatrix, DenseVector}
import mx.cinvestav.commons.events.{AddedNode, Downloaded, EventX, Evicted, Missed, RemovedNode, Replicated, Uploaded}
import cats.implicits._
import mx.cinvestav.commons.types.NodeX

import scala.collection.immutable.ListMap

object Events {
//  case class


  def getOperationsByNodeId(nodeId:String,events:List[EventX]): List[EventX] = events.filter{
    case d:Downloaded => d.selectedNodeId == nodeId
    case u:Uploaded => u.selectedNodeId == nodeId
    case _ => false
  }

  def onlyUploaded(events:List[EventX]): List[EventX] = events.filter{
      case up:Uploaded => true
      case _ => false
    }
  def onlyDownloaded(events:List[EventX]): List[EventX] = events.filter{
    case up:Downloaded => true
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
    Events.onlyDownloaded(events = events).map(_.asInstanceOf[Downloaded]).map{ e=>
      Map(e.selectedNodeId -> Map(e.objectId -> 1)  )
    }
      .foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
      .map{
        case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
      }
  }

  def getHitCounterByNodeV2(events:List[EventX]) = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
   val filtered =  events.filter{
      case _:Downloaded | _:SetDownloads => true
      case _ => false
    }.foldLeft(List.empty[EventX]){
     case (_events,currentEvent)=> currentEvent match {
       case st:SetDownloads => _events.filterNot{
         case d:Downloaded =>d.selectedNodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
         case _=> false
       } :+ st
       case d:Downloaded => _events :+ d
     }
   }.map{
     case d:Downloaded => Map(d.selectedNodeId -> Map(d.objectId -> 1)  )
     case st: SetDownloads => Map(st.nodeId -> Map(st.objectId -> st.counter)  )
   }.foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
     .map{
       case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
     }
    filtered.toList.sortBy(_._1)
  }

  def getGlobalHitRatio(events:List[EventX]): Map[String, Map[String, Double]] = {
     events.filter{
      case _:Downloaded | _:Missed => true
      case _ => false
    }.map{
      case d:Downloaded =>
        Map(d.selectedNodeId->
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
        case _:Downloaded | _:Missed  =>true
        case _ => false
      }.map{
        case d:Downloaded =>
          Map(
            d.selectedNodeId -> Map(
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

  def getObjectIds(events:List[EventX]): List[String] = Events.onlyUploaded(events = events).map(_.asInstanceOf[Uploaded]).map(_.objectId).distinct


  def generateDistributionSchema(events:List[EventX]): Map[String, List[String]] =
    Events.onlyUploaded(events = events).map(_.asInstanceOf[Uploaded]).map{ up=>
      Map(up.objectId -> List(up.selectedNodeId))
    }.foldLeft(Map.empty[String,List[String]])(_ |+| _)

  def toNodeX(events: List[EventX]): List[NodeX] = {
    val nodesWithObjectSizes = Events.nodesToObjectSizes(events)
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
        val element = nodesWithObjectSizes.getOrElse(nodeId,List.empty[Long])
        val usedStorageCapacity = element.sum
        val availableStorageCapacity = node.totalStorageCapacity - usedStorageCapacity
        val totalOfOps = getOperationsByNodeId(nodeId = nodeId,events = events).length
        node.copy(
          usedStorageCapacity = usedStorageCapacity,
          availableStorageCapacity = availableStorageCapacity,
          usedCacheSize = element.length,
          availableCacheSize = node.cacheSize - element.length,
          metadata = Map(
            "TOTAL_REQUEST"-> totalOfOps.toString
          )
        )
    }
  }.toList
  def nodesToObjectSizes(events:List[EventX]): Map[String, List[Long]] =
    onlyUploaded(events).map(_.asInstanceOf[Uploaded]).map{ up =>
      Map(up.selectedNodeId -> List(up.objectSize))
    }.foldLeft(Map.empty[String,List[Long]])(_ |+| _)
  def nodesToObjectIds(events:List[EventX]): Map[String, List[String]] =
    onlyUploaded(events).map(_.asInstanceOf[Uploaded]).map{ up =>
    Map(up.selectedNodeId -> List(up.objectId))
  }.foldLeft(Map.empty[String,List[String]])(_|+|_)

  def filterEvents(events:List[EventX]): List[EventX] = events.foldLeft(List.empty[EventX]){
    case ((events,e))=> e match {
      case _:AddedNode | _:Uploaded | _:Downloaded | _:Replicated | _:Missed | _:SetDownloads => events :+ e
      case ev:Evicted => events.filterNot{
        case up:Uploaded => up.timestamp < ev.timestamp && up.objectId == ev.objectId
        case up:Downloaded => up.timestamp < ev.timestamp && up.objectId == ev.objectId
        case re:Replicated => re.timestamp < ev.timestamp && re.objectId == ev.objectId
        case _=> false
      }
      case rn:RemovedNode => events.filterNot {
        case an: AddedNode => an.timestamp < rn.timestamp && an.addedNodeId == rn.removedNodeId
        case an: Uploaded => an.timestamp < rn.timestamp && an.selectedNodeId == rn.removedNodeId
        case an: Downloaded => an.timestamp < rn.timestamp && an.selectedNodeId == rn.removedNodeId
        case _ => false
      }
    }
  }

}
