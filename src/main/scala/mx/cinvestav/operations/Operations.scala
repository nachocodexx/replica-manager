package mx.cinvestav.operations

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.types
import mx.cinvestav.commons.types.{NodeQueueStats, UploadBalance, UploadResult}
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.types.{Download, DownloadCompleted, How, NodeUFs, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadCompleted, UploadRequest,CompletedOperation}
import mx.cinvestav.commons.balancer.nondeterministic
import mx.cinvestav.commons.utils

object Operations {
  def onlyUpload(operations:List[Operation]): List[Operation] = operations.filter {
    case _:Upload => true
    case _ => false
  }
//  def onlyUploadCompleted(operations:List[Operation]): List[Operation] = operations.filter {
//    case _:UploadCompleted => true
//    case _ => false
//  }
// _____________________________________________________________________________________________________________________
  def onlyDownload(operations:List[Operation]): List[Operation] = operations.filter {
    case _:Download => true
    case _ => false
  }
  def onlyDownloadCompleted(operations:List[CompletedOperation]): List[Operation] = operations.filter {
    case _:DownloadCompleted => true
    case _ => false
  }
// _____________________________________________________________________________________________________________________
//def onlyDownloadAndUploadCompleted(operations:List[CompletedOperation]): List[Com] = operations.filter {
//  case _:DownloadCompleted | _:UploadCompleted => true
//  case _ => false
//}

//  def onlyPendingUpload(operations:List[Operation]) ={
//    val ups = onlyUpload(operations = operations)
//    val upsC = onlyUploadCompleted(completedOperations = operations)
//    val ipsCIds = upsC.map(_.nodeId)
//    ups.filterNot{ up=>
//      ipsCIds.contains(up.nodeId)
//    }
//  }

//  def onlyPendingDownload(operations:List[Operation]) ={
//    val ups = onlyDownload(operations = operations)
//    val upsC = onlyDownloadCompleted(operations = operations)
//    val ipsCIds = upsC.map(_.nodeId)
//    ups.filterNot{ up=>
//      ipsCIds.contains(up.nodeId)
//    }
//  }

  def getAVGServiceTimeNodeIdXCOps(xs:Map[String,List[CompletedOperation]]) = {
    xs.map{
      case (nodeId,xs)=> nodeId ->  {
        val x=  xs.map{
          case dc:DownloadCompleted => dc.serviceTime
          case dc:UploadCompleted => dc.serviceTime
          case _ => Long.MaxValue
        }
        if(x.isEmpty) Long.MaxValue else x.sum.toDouble/x.length.toDouble
      }
    }
  }
  def getAVGServiceTime(operations:List[CompletedOperation]): Map[String, Double] =
    Operations.getAVGServiceTimeNodeIdXCOps(operations.groupBy(_.nodeId))



  def getAVGWaitingTimeNodeIdXCOps(xs:Map[String,List[CompletedOperation]]): Map[String, Double] = {
    xs.map{
      case (nodeId,xs)=> nodeId ->  {
        val x=  xs.map{
          case dc:DownloadCompleted => dc.waitingTime
          case dc:UploadCompleted => dc.waitingTime
          case _ => Long.MaxValue
        }
        if(x.isEmpty) Long.MaxValue else x.sum.toDouble/x.length.toDouble
      }
    }

  }
  def getAVGWaitingTime(operations:List[CompletedOperation]): Map[String, Double] = {
    Operations.getAVGWaitingTimeNodeIdXCOps(operations.groupBy(_.nodeId))
  }


  def processNodes(nodexs:Map[String,NodeX],completedOperations:List[CompletedOperation],queue:Map[String,List[Operation]],objectSize:Long=0L)  = {
    nodexs.map{
      case (nodeId,n) =>
        val ops              = queue.getOrElse(nodeId,List.empty[Operation])
        val upCompleted      = onlyUploadCompleted(completedOperations = completedOperations).filter(_.nodeId == nodeId).asInstanceOf[List[UploadCompleted]]
        val enqueuedUploads   = onlyUpload(operations = ops).asInstanceOf[List[Upload]]
        val upPending         = enqueuedUploads.length
        val dCompleted        = onlyDownloadCompleted(operations = completedOperations).filter(_.nodeId == nodeId).asInstanceOf[List[DownloadCompleted]]
        val enqueuedDownloads = onlyDownload(operations = ops).asInstanceOf[List[Download]]
        val dPending          = enqueuedDownloads.length
//
        val used  =  enqueuedUploads.map(_.objectSize).sum + upCompleted.map(_.objectSize).sum
        val usedD = enqueuedDownloads.map(_.objectSize).sum + dCompleted.map(_.objectSize).sum
      n.copy(
          availableStorageCapacity  = n.totalStorageCapacity - used,
          usedMemoryCapacity        = usedD,
          availableMemoryCapacity   = n.totalMemoryCapacity - usedD,
          ufs                       = NodeUFs(
            nodeId   = n.nodeId,
            diskUF   = nondeterministic.utils.calculateUF(total =  n.totalStorageCapacity,used = used,objectSize= objectSize),
            memoryUF = nondeterministic.utils.calculateUF(total =  n.totalMemoryCapacity,used = usedD,objectSize= objectSize),
            cpuUF    = 0.0
          ),
          metadata = Map(
            "PENDING_UPLOADS" ->upPending.toString,
            "PENDING_DOWNLOADS" ->dPending.toString,
            "COMPLETED_UPLOADS" ->upCompleted.length.toString,
            "COMPLETED_DOWNLOADS" ->dCompleted.length.toString
      ) ++ n.metadata
        )
    }.map(n=> n.nodeId -> n)
  }
//  __________________________________________________________________
  def uploadBalance(x:String,nodexs:Map[String,NodeX])(
    operations:List[Operation] = Nil,
    queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],
    completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]],
    objectSize:Long,rf:Int = 1
) = {
    x match {
      case "MIN_WAITING_TIME"  =>
        val defaultWtXNode   = nodexs.keys.toList.map(_ -> 0.0).toMap
        val waitingTimeXNode =  (defaultWtXNode ++ Operations.getAVGWaitingTimeNodeIdXCOps(completedQueue)).toList.sortBy(_._2)
        println(waitingTimeXNode)
        waitingTimeXNode.take(rf).map(_._1).map(nodexs).map(n=>Operations.updateNodeX(nodeX = n , objectSize = objectSize, downloadDiv =0L))
      case "ROUND_ROBIN" =>
        val grouped  = onlyUpload(operations).asInstanceOf[List[Upload]].groupBy(_.nodeId)
//        println(grouped)
        val xs       = grouped.map(x=> x._1 -> x._2.length)
        val total    = xs.values.toList.sum
        val AR       = nodexs.size
        val selectedNodes = (0 until rf).toList.map(i => (i + (total % AR))%AR )
        val sortedNodes = nodexs.values.toList.sortBy(_.nodeId)
        selectedNodes.map(sortedNodes)
          .map(n=> Operations.updateNodeX(n,objectSize = objectSize,downloadDiv = 0L))

      case "SORTING_UF" =>
        nodexs.values.toList.sortBy(_.ufs.diskUF).take(rf)
        .map(n=> Operations.updateNodeX(n,objectSize = objectSize,downloadDiv = 0L))
    }
  }
  def downloadBalance(x:String,nodexs:Map[String,NodeX])(
    operations:List[Operation] = Nil,
    queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],
    completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]],
    objectSize:Long,
    rf:Int = 1
  ) = {
    x match {
      case "MIN_WAITING_TIME"  =>
        val defaultWtXNode   = nodexs.keys.toList.map(_ -> 0.0).toMap
        val waitingTimeXNode =  (defaultWtXNode ++ Operations.getAVGWaitingTimeNodeIdXCOps(completedQueue)).toList.sortBy(_._2)
        waitingTimeXNode.take(rf).map(_._1).map(nodexs).map(n=>Operations.updateNodeX(nodeX = n , objectSize = objectSize, uploadDiv =0L))
      case "ROUND_ROBIN" =>
        val grouped  = onlyDownload(operations).asInstanceOf[List[Download]].groupBy(_.nodeId)
        val xs       = grouped.map(x=> x._1 -> x._2.length)
        val total    = xs.values.toList.sum
        val AR       = nodexs.size
        val selectedNodes = (0 until rf).toList.map(i => (i + (total % AR))%AR )
        selectedNodes.map(nodexs.values.toList.sortBy(_.nodeId))
          .map(n=> Operations.updateNodeX(n,objectSize = objectSize))

      case "SORTING_UF" =>
        nodexs.values.toList.sortBy(_.ufs.memoryUF).take(rf)
          .map(n=> Operations.updateNodeX(n,objectSize = objectSize))
    }
  }





  def updateNodeX(nodeX: NodeX,objectSize:Long, uploadDiv:Long = 1L, downloadDiv:Long = 1L )={
    val usedStorageCapacity      = nodeX.usedStorageCapacity + (objectSize*uploadDiv)
//    println(usedStorageCapacity,nodeX.usedStorageCapacity)
    val usedMemoryCapacity       = nodeX.usedMemoryCapacity + (objectSize*downloadDiv)
    val availableStorageCapacity = nodeX.totalStorageCapacity - usedStorageCapacity
    val availableMemoryCapacity  = nodeX.totalMemoryCapacity - usedMemoryCapacity
    nodeX.copy(
        usedStorageCapacity = usedStorageCapacity,
        usedMemoryCapacity = usedMemoryCapacity,
        availableStorageCapacity = availableStorageCapacity,
        availableMemoryCapacity = availableMemoryCapacity,
        ufs =  nodeX.ufs.copy(
          diskUF = nondeterministic.utils.calculateUF(
            total = nodeX.totalStorageCapacity,
            used = nodeX.usedStorageCapacity,
            objectSize = objectSize*uploadDiv
          ),
          memoryUF = nondeterministic.utils.calculateUF(
            total = nodeX.totalMemoryCapacity,
            used = nodeX.usedMemoryCapacity,
            objectSize = objectSize*downloadDiv
          )
        )
    )
  }


  def processUploadRequest(
                            lbToken:String="SORTING_UF",
                            operations:List[Operation],
                            queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],
                            completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]]
                          )(ur: UploadRequest,nodexs:Map[String,NodeX]) = {
    val xx = ur.what.foldLeft((nodexs,List.empty[ReplicationSchema] )) {
      case (x, w) =>
        val ns = x._1
        val rf            = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)

        val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
        val selectedNodes = Operations.uploadBalance(lbToken, ns)(
          operations = operations,
          objectSize = objectSize, rf = rf,
          queue = queue,
          completedQueue = completedQueue
        )
//        println(selectedNodes)
//        val xs = selectedNodes
        //          val xs            = selectedNodes.map(n => Operations.updateNodeX(n, objectSize))
        val y             = selectedNodes.foldLeft(ns) { case (xx, n) => xx.updated(n.nodeId, n)}
        val yy            = selectedNodes.map(_.nodeId).map(y).toList
        val pivotNode     = yy.head
        val where         = yy.tail.map(_.nodeId)
        val rs = ReplicationSchema(
          nodes = Nil,
          data = Map(pivotNode.nodeId -> ReplicationProcess(what = w::Nil,where =where,how = How("ACTIVE","PUSH"),when = "REACTIVE" ))
        )
        ( y, x._2 :+ rs )
    }
    xx
  }


  def processRSAndUpdateQueue(clientId:String)(rs:ReplicationSchema)(implicit ctx:NodeContext) = {
    rs.data.toList.traverse{
      case (nodeId, rp) =>
        for {
          _              <- IO.unit
          what           = rp.what
          where          = rp.where
          whereCompleted = where :+ nodeId
          operations     <- what.traverse{ w=>
            val opId     = utils.generateNodeId(prefix = "op",autoId = true,len = 15)
            for {
              arrivalTime  <- IO.monotonic.map(_.toNanos)
              currentState <- ctx.state.get
              queue        = currentState.nodeQueue
              objectSize   = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
              up           = Upload(
                operationId = "OPERATION_ID",
                serialNumber = -1,
                arrivalTime =arrivalTime ,
                objectId = w.id,
                objectSize = objectSize,
                clientId = clientId,
                nodeId = "NODE_ID",
                metadata = Map.empty[String,String]
              )
              ops          = whereCompleted.foldLeft( (queue,List.empty[Operation]) ){
                case (x,n)=>
                  val queues = x._1
                  val q = queues.getOrElse(n,Nil)
                  val completed = currentState.completedQueue.getOrElse(n,Nil)
                  val operationId = utils.generateNodeId(prefix = "op",autoId = true,len = 15)
                  val op = up.copy(serialNumber = q.length+completed.length,nodeId = n,operationId = operationId)
                  (
                    queues.updated(n,q :+ op),
                    x._2:+op
                  )
              }
              _            <- ctx.state.update{ s=>
                s.copy(nodeQueue =  ops._1)
              }
            } yield ops._2
          }.map(_.flatten)
        } yield operations
    }.map(_.flatten)
  }

  def distributionSchema(
                          operations:List[Operation],
                          completedOperations: List[CompletedOperation],
                         queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],technique:String = "ACTIVE") ={
    val objectIdXNodes = Operations.onlyUpload(queue.values.flatten.toList).asInstanceOf[List[Upload]].groupBy(_.objectId)
//      Operations.onlyUpload(operations).asInstanceOf[List[Upload]].groupBy(_.objectId)

    completedOperations.groupBy(_.objectId).map{
        case (oId,cOps) =>
          val pendings = objectIdXNodes.getOrElse(oId,Nil)
          val partialDs = cOps.map(_.nodeId).distinct
          technique match {
              case "ACTIVE" =>
                if(pendings.isEmpty) (oId ->partialDs) else (oId -> Nil)
              case "PASSIVE" => oId -> partialDs
          }
    }
  }
  def onlyUploadCompleted(completedOperations:List[CompletedOperation])=  {
    completedOperations.filter{
      case _:UploadCompleted =>  true
    }
  }

  def getObjectIds(completedOperations:List[CompletedOperation]) = {
    onlyUploadCompleted(completedOperations = completedOperations)
  }

  def generateUploadBalance(xs:Map[String,List[Operation]])(implicit ctx:NodeContext):IO[UploadBalance] = {
    for  {
      currentState         <- ctx.state.get
      operations           = currentState.completedOperations
      mergeOps             = xs.values.flatten
      groupedByOId         = mergeOps.filter {
        case _:types.Download => true
        case _:Upload => true
        case _ => false
      }.groupBy {
        case d:types.Download => d.objectId
        case u:Upload => u.objectId
        case _ => ""
      }
      avgServiceTimeByNode = Operations.getAVGServiceTime(operations = operations)
      avgWaitingTimeByNode = Operations.getAVGWaitingTime(operations = operations)
      id                   = utils.generateNodeId(prefix = "ub",len = 10,autoId = true)
      xxs = groupedByOId.map{
        case (objectId, value) =>
          val id     = utils.generateNodeId(prefix = "us",len = 10,autoId = true)
          val nodeq  = value.map{ x=>
            NodeQueueStats(
              operationId =x.operationId ,
              nodeId = x.nodeId,
              avgServiceTime = avgServiceTimeByNode.getOrElse(x.nodeId,Double.MaxValue),
              avgWaitingTime = avgWaitingTimeByNode.getOrElse(x.nodeId,Double.MaxValue),
              queuePosition = x.serialNumber
            )
          }.toList
          val correlationId = value.map(_.correlationId).headOption.getOrElse("CORRELATION_ID")

          objectId -> UploadResult(id = correlationId, results = nodeq)
      }
      res                  = UploadBalance(id = id, result = xxs, serviceTime = 0)
    } yield res
  }




}
