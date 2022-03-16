package mx.cinvestav.events

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{Del, PutCompleted, UpdatedNodePort}
import mx.cinvestav.commons.types.Monitoring

import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Random
//
import breeze.linalg._
import mx.cinvestav.commons.types.DumbObject
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
  import mx.cinvestav.commons.types.Monitoring.NodeInfo
  case class UpdatedNetworkCfg(
                                nodeId: String,
                                poolId:String,
                                timestamp: Long,
                                publicPort:Int,
                                ipAddress:String,
                                monotonicTimestamp: Long = 0L,
                                correlationId: String = UUID.randomUUID().toString,
                                serialNumber: Long=0,
                                eventType: String ="UPDATED_PUBLIC_PORT",
                                eventId: String= UUID.randomUUID().toString,
                                serviceTimeNanos: Long=0L,
                              ) extends EventX
  case class CollectedNodesInfo(
                        nodeId: String,
                        poolId:String,
                        infos:List[NodeInfo],
                        timestamp: Long,
                        monotonicTimestamp: Long = 0L,
                        correlationId: String = UUID.randomUUID().toString,
                        serialNumber: Long=0,
                        eventType: String ="COLLECTED_NODES_INFO",
                        eventId: String= UUID.randomUUID().toString,
                        serviceTimeNanos: Long=0L,
                      ) extends EventX

  case class HotObject(
                        serialNumber: Long,
                        nodeId: String,
                        timestamp: Long,
                        monotonicTimestamp: Long,
                        objectId:String,
                        objectSize:Long,
                        correlationId: String,
                        eventType: String ="HOT_OBJECT",
                        eventId: String= UUID.randomUUID().toString,
                        serviceTimeNanos: Long=0L,
                      ) extends EventX

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
//  case class DumbObject(objectId:String,objectSize:Long)
  case class MeasuredServiceTime(
                              serialNumber: Long,
                              nodeId: String,
                              timestamp: Long,
                              monotonicTimestamp: Long,
                              objectId:String,
                              objectSize:Long,
                              correlationId: String,
                              eventType: String ="MEASURED_SERVICE_TIME",
                              eventId: String= UUID.randomUUID().toString,
                              serviceTimeNanos: Long=0L,
                                ) extends EventX

  case class MonitoringStats(
                              serialNumber: Long,
                              nodeId: String,
                              timestamp: Long,
                              monotonicTimestamp: Long,
                              jvmTotalRAM:Double,
                              jvmFreeRAM:Double,
                              jvmUsedRAM:Double,
                              jvmUfRAM:Double,
                              totalRAM:Double,
                              freeRAM:Double,
                              usedRAM:Double,
                              ufRAM:Double,
                              systemCPULoad:Double,
                              cpuLoad:Double,
                              correlationId: String= UUID.randomUUID().toString,
                              eventType: String ="MONITORING_STATS",
                              eventId: String= UUID.randomUUID().toString,
                              serviceTimeNanos: Long=0L,
                            ) extends EventX


//  onlyUpdatedPublicPort
  def onlyUpdatedPublicPort(events:List[EventX]): List[EventX] = events.filter {
      case _:UpdatedNetworkCfg => true
      case _=> false
    }
  def getPublicPort(events:List[EventX],nodeId:String): Option[UpdatedNetworkCfg] = onlyUpdatedPublicPort(events=events)
    .map(_.asInstanceOf[UpdatedNetworkCfg])
    .find(_.nodeId == nodeId)
//  Get interval downloads
  def getDownloadsByIntervalByObjectId(objectId:String)(period:FiniteDuration)(events:List[EventX]) = {
    val es = onlyGets(events=events).map(_.asInstanceOf[Get]).filter(_.objectId == objectId).map(_.asInstanceOf[EventX])
    Events.getDownloadsByInterval(period)(events=es).map(_.length)
  }
  def getDownloadsByInterval(period:FiniteDuration)(events:List[EventX]): List[List[EventX]] = {
    val downloads         = onlyGets(events = events).sortBy(_.monotonicTimestamp)
    val maybeMinMonotonicTimestamp  = downloads.minByOption(_.monotonicTimestamp).map(_.monotonicTimestamp)
    maybeMinMonotonicTimestamp match {
      case Some(minMonotonicTimestamp) =>
        @tailrec
        def inner(es:List[List[EventX]], initT:FiniteDuration, i:Int=0):List[List[EventX]]= {
          val efs = es.flatten
          val L = efs.length
          if(L == downloads.length) es
          else {
            val newEs = downloads.toSet.diff(efs.toSet).toList
            if(newEs.isEmpty) es
            else {
              val newI  = i + 1
              val newT  =  (initT.toNanos+ period.toNanos).nanos
              inner(es = es:+newEs.filter(_.monotonicTimestamp <= initT.toNanos) , initT = newT ,i = newI)
            }
          }
        }
        inner(es= Nil,initT = (minMonotonicTimestamp + period.toNanos).nanos, i = 1)
      case None => Nil
    }
  }

//
  def getNodeMonitoringStats(events:List[EventX])= {
    val xs = events.filter{
      case x:MonitoringStats=>true
      case _=> false
    }.map(_.asInstanceOf[MonitoringStats])
    val ys = xs.groupBy(_.nodeId).map{
      case (nodeId,ms)=>nodeId -> ms.sortBy(_.monotonicTimestamp).lastOption.getOrElse(
        MonitoringStats(serialNumber = 0,nodeId=nodeId,
          timestamp = 0L,
          monotonicTimestamp = 0L,
          jvmTotalRAM = 0.0,
          jvmFreeRAM = 0.0,
          jvmUsedRAM = 0.0,
          jvmUfRAM = 0.0,
          totalRAM = 0.0,
          freeRAM = 0.0,
          usedRAM = 0.0,
          ufRAM = 0.0,
          systemCPULoad = 0.0,
          cpuLoad = 0.0,
        )
      )
    }
    ys.map{
      case (nodeId,ms)=>nodeId -> Map(
        "JvmTotalRAM"->ms.jvmTotalRAM.toString,
        "JvmFreeRAM"->ms.jvmFreeRAM.toString,
        "JvmUsedRAM"->ms.jvmUsedRAM.toString,
        "JvmUfRAM"->ms.jvmUfRAM.toString,
        "totalRAM"->ms.totalRAM.toString,
        "freeRAM"->ms.freeRAM.toString,
        "usedRAM"->ms.usedRAM.toString,
        "UfRAM"->ms.ufRAM.toString,
        "systemCPULoad"->ms.systemCPULoad.toString,
        "CPULoad"->ms.cpuLoad.toString,
      )
    }
  }

  def getHotObjectCounter(objectId:String,events:List[EventX],lastMonotonic:Long) = {
    events.count{
      case x:HotObject=> (x.objectId == objectId) && (x.monotonicTimestamp > lastMonotonic)
      case _ => false
    }
  }
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

//  def calculateMemoryUFByNode(events:List[EventX],objectSize:Long): Map[String, Double] = {
//    val nodes = getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
//    val initNodesUFs = nodes.map{
//      case (nodeId, _) => nodeId->0.0
//    }
//   initNodesUFs |+| events.filter{
//      case _:Get | _:GetInProgress => true
//      case _ => false
//    }.groupBy(_.nodeId).map{
//      case (nodeId, events) =>  nodeId ->
//        events.groupBy(_.correlationId)
//          .map{
//            case (_, es) if es.length==1  => es.map{
//              case x:GetInProgress => x.objectSize
//              case  _ => 0
//            }.sum
//            case (_,_) => 0
//          }.sum
//    }.map{
//      case (nodeId, usedMemory) =>
//        val maybeNode = nodes.get(nodeId)
//        maybeNode match {
//          case Some(node) => node.nodeId -> UF.calculate(
//            total = node.totalMemoryCapacity,
//            used = objectSize+usedMemory ,
//            objectSize = objectSize
//          )
//          case None => ""->1.0
//        }
//    }
//      .filter(_._1.nonEmpty)
//  }

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
  def balanceByReplica(
                        downloadBalancer:String="ROUND_ROBIN",
                        objectSize:Long=0)(
                        objectId:String,
                        arMap:Map[String,NodeX],
                        events:List[EventX],
                        infos:List[Monitoring.NodeInfo] = Nil
  )(implicit ctx:NodeContext):Option[NodeX] = {
    import mx.cinvestav.commons.balancer.{nondeterministic,deterministic}
    if(arMap.size == 1) arMap.head._2.some
    else downloadBalancer match {
      case "LEAST_CONNECTIONS" =>
        val nodeWithReplicaCounter = getReplicaCounter(objectId,events,arMap)
        nodeWithReplicaCounter.minByOption(_._2).flatMap{
          case (nodeId, _) => arMap.get(nodeId)
        }
      case "ROUND_ROBIN" =>
        val nodeIds = NonEmptyList.fromListUnsafe(arMap.keys.toList)
        val counter = Events.onlyGets(events=events).map(_.asInstanceOf[Get]).filter(_.objectId==objectId).groupBy(_.nodeId).map{
            case (nodeId,xs)=>nodeId -> xs.length
          }
        val defaultCounter:Map[String,Int] = nodeIds.toList.map(x=>x->0).toMap
        val selectedNodeId = deterministic.RoundRobin(nodeIds = NonEmptyList.fromListUnsafe(arMap.keys.toList)  )
          .balanceWith(
            nodeIds = nodeIds.sorted,
            counter = counter |+| defaultCounter
          )
        arMap.get(selectedNodeId)
//        val nodeWithReplicaCounter = getReplicaCounter(guid,events,arMap)
//        val totalOfReqs = nodeWithReplicaCounter.values.sum
//        val index = totalOfReqs % nodeWithReplicaCounter.size
//        val nodeId = nodeWithReplicaCounter.toList.get(index).map(_._1).get
//        arMap.get(nodeId)
      case "SORTING_UF" =>
        None
//        val filteredInfos = infos.filter(x=>arMap.keys.toList.contains(x.nodeId))
//        val selectedNodeId = nondeterministic
//          .SortingUF()
//          .balance(
//            infos = NonEmptyList.fromListUnsafe(filteredInfos),
//            objectSize = objectSize,
//            takeN = 1,
////            mapTotalFn = _.RAMInfo.total,
////            mapUsedFn = _.RAMInfo.used
//          )

//        selectedNodeId.headOption.flatMap(arMap.get)
//        arMap.get(selectedNodeId.head)
      case "TWO_CHOICES" =>
        val filteredInfos = infos.filter(x=>arMap.keys.toList.contains(x.nodeId))
        val selectedNodeId = nondeterministic
          .TwoChoices(psrnd = deterministic.PseudoRandom(nodeIds = NonEmptyList.fromListUnsafe(arMap.keys.toList)))
          .balance(info = NonEmptyList.fromListUnsafe(filteredInfos),
            objectSize = objectSize,
            mapTotalFn = _.RAMInfo.total,
            mapUsedFn = _.RAMInfo.used
          )
        arMap.get(selectedNodeId)
//
      case "PSEUDO_RANDOM" =>
        val nodeIds = NonEmptyList.fromListUnsafe(arMap.keys.toList)
        val selectedNodeId = deterministic.PseudoRandom(
          nodeIds = nodeIds.sorted
        ).balance
        arMap.get(selectedNodeId)
//        val nodeIds =  arMap.keys.toList
//        val randomIndex = new Random().nextInt(arMap.size)
//        val randomNodeId = nodeIds(randomIndex)
//        arMap.get(randomNodeId)
      case _ =>
        val nodeWithReplicaCounter = getReplicaCounter(objectId,events,arMap)
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
            case x:HotObject => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:MonitoringStats => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:UpdatedNetworkCfg => x.copy(
              monotonicTimestamp = now,
              serialNumber = lastSerialNumber+index
            )
            case x:PutCompleted => x.copy(
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

  def saveMonitoringEvents(event:EventX)(implicit ctx:NodeContext) = for {
    currentState     <- ctx.state.get
    //    currentEvents    = currentState.monitoringEvents
    currentEvents    = currentState.monitoringEx
    lastSerialNumber = currentEvents.minByOption(_._2.serialNumber).map(_._2.asInstanceOf[MonitoringStats]).map(_.serialNumber.toInt).getOrElse(0)
    transformeEvent <- sequentialMonotonic(lastSerialNumber ,events=event::Nil).map(_.head)
    xs               = currentState.monitoringEx.updatedWith(event.nodeId)(x=> transformeEvent.some)
    _ <- ctx.state.update(s=>s.copy(monitoringEx = xs ))
//    _                <- ctx.state.update{ s=>s.copy(monitoringEvents =  s.monitoringEvents ++ transformeEvents )}
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

  def isPending(objectId:String,events:List[EventX]) = {
    !EventXOps.onlyPutCompleteds(events=events).map(_.asInstanceOf[PutCompleted])
      .map(_.objectId).contains(objectId)
  }
  def getObjectByIdV3(objectId:String,events:List[EventX]):Option[DumbObject] ={
    val x                  = EventXOps.onlyPendingPuts(events =events)
      .map(_.asInstanceOf[Put])
      .find(_.objectId == objectId)
      .map(x=>DumbObject(x.objectId,x.objectSize))
    x
//    x.map(x=> (x,pending))
//    val completedObjectIds = x.map(_.objectId)
//    val y = EventXOps.onlyPendingPuts(events = events).map(_.asInstanceOf[Put])
//    y
//      .find(p =>completedObjectIds.contains(p.objectId))
//      .map(x=>DumbObject(x.objectId,x.objectSize))
  }
  def getObjectByIdV2(objectId:String,events:List[EventX]):Option[DumbObject] =
    EventXOps.onlyPutCompleteds(events=events)
    .map(_.asInstanceOf[PutCompleted]).find(_.objectId == objectId)
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

  def onlyPutsAndGets(events:List[EventX])  = events.filter{
      case _:Put => true
      case _:Get => true
      case _ => false
    }
  def getLastOperation(events:List[EventX]) = onlyPutsAndGets(events=events).sortBy(_.monotonicTimestamp).lastOption
  def getOperationCounter(events:List[EventX]) = onlyPutsAndGets(events=events).length


  def getLastWaitingTime(events:List[EventX]): Long = {
    val lastOp = getLastOperation(events=events)
    lastOp match {
      case Some(p:Put) => 0L
      case Some(g:Get) => 0L
      case _ => 0L
    }
  }
  def onlyPutos(events:List[EventX]): List[EventX] = events.filter{
      case up:Put => true
      case _ => false
    }
  def onlyGets(events:List[EventX]): List[EventX] = events.filter{
    case up:Get => true
    case _ => false
  }

  def getNodeIds(events: List[EventX]) =
    onlyAddedNode(events = events)
      .map(_.asInstanceOf[AddedNode])
      .map(_.addedNodeId)
      .distinct
      .sorted
//

  def getHitCounterByNodeV2(events:List[EventX], windowTime:Long=0): Map[String, Map[String, Int]] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
   val filtered =  events.filter{
      case e@(_:Get | _:SetDownloads) => e.monotonicTimestamp > windowTime
//        || e.monotonicTimestamp > windowTime
//      case _:Get | _:SetDownloads => true
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
  def generateMatrix(events:List[EventX]): DenseMatrix[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNode(events = events).toList.sortBy(_._1):_*
    ).map{
      case (nodeId, counter) => nodeId-> ListMap(counter.toList.sortBy(_._1):_*)
    }
    val vectors = hitCounter
      .toList
      .sortBy(_._1)
      .map{
        case (nodeId, objectDownloadCounter) =>
          val values = objectDownloadCounter.values.toArray.map(_.toDouble)
          val vec = DenseVector(values:_*)
          vec
    }.toList
    DenseMatrix(vectors.map(_.toArray):_*)
  }

  def generateMatrixV2(events:List[EventX], windowTime:Long=0): DenseMatrix[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNodeV2(events = events,windowTime = windowTime).toList.sortBy(_._1):_*
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

  def generateReplicaUtilization(events:List[EventX]) = {
    val x                = Events.generateMatrixV2(events = events,windowTime = 0)
    val sumA             = (sum(x(::,*)).t).mapValues(x=>if(x==0) 1 else x)
    val xx = x(*,::) / sumA
    xx
  }
  def generateTemperatureMatrix(events:List[EventX]): DenseMatrix[Double] =  {
    val x = Events.generateMatrix(events=events)
    val dividend = sum(x(*,::))
    x(::,*)  / dividend
  }
  def generateTemperatureMatrixV2(events:List[EventX],windowTime:Long): DenseMatrix[Double] =  {
    val x = Events.generateMatrixV2(events=events,windowTime = windowTime)
    val dividend = sum(x(*,::)).mapValues(x=>if(x==0) 1 else x)
    x(::,*)  / dividend
  }
  def generateTemperatureMatrixV3(events:List[EventX],windowTime:Long): DenseMatrix[Double] =  {
    val x = Events.generateMatrixV2(events=events,windowTime = windowTime)
    x
//    val dividend = sum(x(*,::))
//    x(::,*)
  }

  def getObjectIds(events:List[EventX]): List[String] = Events.onlyPutos(events = events).map(_.asInstanceOf[Put]).map(_.objectId).distinct

  def getDumbObject(events:List[EventX]): List[DumbObject] = Events.onlyPutos(events = events)
    .map(_.asInstanceOf[Put]).map{
    x=> DumbObject(x.objectId,x.objectSize)
  }.distinctBy(_.objectId)
//    .map(_.objectId).distinct


  def generateDistributionSchema(events:List[EventX]): Map[String, List[String]] =
    Events.onlyPutos(events = events).map(_.asInstanceOf[Put]).map{ up=>
      Map(up.objectId -> List(up.nodeId))
    }.foldLeft(Map.empty[String,List[String]])(_ |+| _)

  def getOriginalReplica(events:List[EventX]) = {
    val x = Events.onlyPutos(events = events).map(_.asInstanceOf[Put]).groupBy(_.objectId)
    val y = x.map{
      case (objectId, puts) => objectId -> puts.minBy(_.monotonicTimestamp).nodeId
    }.toMap
    y
//      .map{ up=>
//      Map(up.objectId ->  List(up))
//    }.foldLeft(Map.empty[String,List[Put]])(_ |+| _)

  }

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

  def onlyAddedNode(events:List[EventX]): List[EventX] = events.filter{
    case _:AddedNode => true
    case _ => false
  }

  def nodesToDumbObject(events:List[EventX]): Map[String, List[DumbObject]] =
    onlyPutos(events).map(_.asInstanceOf[Put]).map{ up =>
      Map(up.nodeId -> List( DumbObject(up.objectId,up.objectSize)  ))
    }.foldLeft(Map.empty[String,List[DumbObject]])(_|+|_)

  def getOperationsByNodeId(nodeId:String,events:List[EventX]): List[EventX] = events.filter{
    case d:Get => d.nodeId == nodeId
    case u:Put => u.nodeId == nodeId
    case _ => false
  }
//  Get all current storage nodes
  def getAllNodeXs(events: List[EventX]): List[NodeX] = {
    val nodesAndDumbObjects   = nodesToDumbObject(events)
    onlyAddedNode(events).map(_.asInstanceOf[AddedNode])
      .map{ an =>
        an.addedNodeId -> NodeX(
          nodeId = an.addedNodeId,
          ip=an.ipAddress,
          port=an.port,
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
        val dumbObjs = nodesAndDumbObjects.getOrElse(nodeId,List.empty[DumbObject])
//        val usedStorageCapacity = nodesAndDumbObjects.getOrElse(nodeId,List.empty[DumbObject]).map(_.objectSize).sum
        val usedStorageCapacity = dumbObjs.map(_.objectSize).sum
        val availableStorageCapacity = node.totalStorageCapacity - usedStorageCapacity
        val totalGets = getOperationsByNodeId(nodeId = nodeId, events = events).count {
          case _: Get => true
          case _ => false
        }
        val totalPuts = getOperationsByNodeId(nodeId = nodeId, events = events).count {
          case _: Put => true
          case _ => false
        }
        node.copy(
          usedStorageCapacity = usedStorageCapacity,
          availableStorageCapacity = availableStorageCapacity,
          usedCacheSize = dumbObjs.length,
          availableCacheSize = node.cacheSize - dumbObjs.length,
          metadata = Map(
            "TOTAL_REQUESTS"-> (totalGets+totalPuts).toString,
            "DOWNLOAD_REQUESTS"-> totalGets.toString,
            "UPLOAD_REQUESTS"-> totalPuts.toString,
          ),
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


  def orderAndFilterEvents(events:List[EventX]):List[EventX] =
    Events.filterEvents(EventXOps.OrderOps.byTimestamp(events=events).reverse)

  def orderAndFilterEventsMonotonic(events:List[EventX]):List[EventX] =
    filterEventsMonotonic(events.sortBy(_.monotonicTimestamp))

  def orderAndFilterEventsMonotonicV2(events:List[EventX]):List[EventX] =
    filterEventsMonotonicV2(events.sortBy(_.monotonicTimestamp))
//    Events.filterEvents(EventXOps.OrderOps.byTimestamp(events=events).reverse)


  def filterEventsMonotonicV2(events:List[EventX],predicate:EventX =>Boolean = _.eventType != "HOT_OBJECT"): List[EventX] = {
    var newEvents = collection.mutable.ListBuffer.empty[EventX]

    events.filter(predicate).foreach {
      case ev: Evicted =>
       newEvents = newEvents.filterNot {
          case get: Get => get.monotonicTimestamp < ev.monotonicTimestamp && get.objectId == ev.objectId && (get.nodeId == ev.fromNodeId || get.nodeId == ev.nodeId)
          case put: Put => put.monotonicTimestamp < ev.monotonicTimestamp && put.objectId == ev.objectId && (put.nodeId == ev.fromNodeId || put.nodeId == ev.nodeId)
          case rep: Replicated => rep.monotonicTimestamp < ev.monotonicTimestamp && rep.objectId == ev.objectId && (rep.nodeId == ev.fromNodeId || rep.nodeId == ev.nodeId)
          case _ => false
        }
      case rn: RemovedNode =>
        newEvents = newEvents.filterNot {
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
