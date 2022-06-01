package mx.cinvestav.operations

import cats.implicits._
import cats.effect._
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
  def onlyUploadCompleted(operations:List[Operation]): List[Operation] = operations.filter {
    case _:UploadCompleted => true
    case _ => false
  }
// _____________________________________________________________________________________________________________________
  def onlyDownload(operations:List[Operation]): List[Operation] = operations.filter {
    case _:Download => true
    case _ => false
  }
  def onlyDownloadCompleted(operations:List[Operation]): List[Operation] = operations.filter {
    case _:DownloadCompleted => true
    case _ => false
  }
// _____________________________________________________________________________________________________________________
//def onlyDownloadAndUploadCompleted(operations:List[CompletedOperation]): List[Com] = operations.filter {
//  case _:DownloadCompleted | _:UploadCompleted => true
//  case _ => false
//}

  def onlyPendingUpload(operations:List[Operation]) ={
    val ups = onlyUpload(operations = operations)
    val upsC = onlyUploadCompleted(operations = operations)
    val ipsCIds = upsC.map(_.nodeId)
    ups.filterNot{ up=>
      ipsCIds.contains(up.nodeId)
    }
  }

  def onlyPendingDownload(operations:List[Operation]) ={
    val ups = onlyDownload(operations = operations)
    val upsC = onlyDownloadCompleted(operations = operations)
    val ipsCIds = upsC.map(_.nodeId)
    ups.filterNot{ up=>
      ipsCIds.contains(up.nodeId)
    }
  }

  def getAVGServiceTime(operations:List[CompletedOperation]): Map[String, Double] = {
//    val upAndDownC = onlyDownloadAndUploadCompleted(operations = operations).groupBy(_.nodeId)
    operations.groupBy(_.nodeId).map{
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
//  def getWaitingTimeBySerialNumber(operations:List[CompletedOperation],serialNumber:Int) = {
//    onlyDownloadAndUploadCompleted(operations = operations).find(_.serialNumber == serialNumber) match {
//      case Some(value) => value match {
//        case DownloadCompleted(operationId, serialNumber, arrivalTime, serviceTime, waitingTime,idleTime, objectId, nodeId, metadata) => waitingTime
//        case UploadCompleted(operationId, serialNumber, arrivalTime, serviceTime, waitingTime,idleTime,objectId, nodeId, metadata) => waitingTime
//        case _ => 0
//      }
//      case None => 0
//    }
//  }


  def processNodes(nodexs:Map[String,NodeX],operations:List[Operation],objectSize:Long=0L)  = {
    nodexs.map{
      case (nodeId,n) =>
//        val upCompletedIds = upCompleted.map(_.nodeId)
        val upCompleted    = onlyUploadCompleted(operations = operations)
        val upPending      = onlyPendingUpload(operations = operations)
        val dCompleted    = onlyDownloadCompleted(operations = operations)
        val dPending      = onlyPendingDownload(operations = operations)
        val uploads   = onlyUpload(operations = operations).asInstanceOf[List[Upload]]
        val used      = uploads.map(_.objectSize).sum
        val downloads = onlyDownload(operations = operations).asInstanceOf[List[Download]]
        val usedD     = downloads.map(_.objectSize).sum
//        usedSto        val upCompleted    = onlyUploadCompleted(operations = operations)
//        val upPending      = onlyPendingUpload(operations = operations)rageCapacity       = used,
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
            "PENDING_UPLOADS" ->upPending.length.toString,
            "PENDING_DOWNLOADS" ->dPending.length.toString,
            "COMPLETED_UPLOADS" ->upCompleted.length.toString,
            "COMPLETED_DOWNLOADS" ->dCompleted.length.toString
      ) ++ n.metadata
        )
    }.map(n=> n.nodeId -> n)
  }
  def uploadBalance(x:String,nodexs:Map[String,NodeX])(operations:List[Operation],objectSize:Long,rf:Int = 1) = {
    x match {
      case "ROUND_ROBIN" =>
        val grouped  = onlyUpload(operations).asInstanceOf[List[Upload]].groupBy(_.nodeId)
        val xs       = grouped.map(x=> x._1 -> x._2.length)
        val total    = xs.values.toList.sum
        val AR       = nodexs.size
        val selectedNodes = (0 until rf).toList.map(i => (i + (total % AR))%AR )
        selectedNodes.map(nodexs.values.toList.sortBy(_.nodeId))
          .map(n=> Operations.updateNodeX(n,objectSize = objectSize))

      case "SORTING_UF" =>
        nodexs.values.toList.sortBy(_.ufs.diskUF).take(rf)
        .map(n=> Operations.updateNodeX(n,objectSize = objectSize))
    }
  }

  def updateNodeX(nodeX: NodeX,objectSize:Long)={
    val usedStorageCapacity      = nodeX.usedStorageCapacity + objectSize
//    println(usedStorageCapacity,nodeX.usedStorageCapacity)
    val usedMemoryCapacity       = nodeX.usedMemoryCapacity + objectSize
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
            objectSize = objectSize
          ),
          memoryUF = nondeterministic.utils.calculateUF(
            total = nodeX.totalMemoryCapacity,
            used = nodeX.usedMemoryCapacity,
            objectSize = objectSize
          )
        )
    )
  }


  def processUploadRequest(lbToken:String="SORTING_UF",operations:List[Operation])(ur: UploadRequest,nodexs:Map[String,NodeX]) = {
    val xx = ur.what.foldLeft((nodexs,List.empty[ReplicationSchema] )) {
      case (x, w) =>
        val ns = x._1
        val rf = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)

        val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
        val selectedNodes = Operations.uploadBalance(lbToken, ns)(operations = operations, objectSize = objectSize, rf = rf)
        val xs = selectedNodes
        //          val xs            = selectedNodes.map(n => Operations.updateNodeX(n, objectSize))
        val y             = xs.foldLeft(ns) { case (xx, n) => xx.updated(n.nodeId, n)}
        val yy            = selectedNodes.map(_.nodeId).map(y)
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
            val opId     = utils.generateNodeId(prefix = "op",autoId = true)
            for {
              arrivalTime  <- IO.monotonic.map(_.toNanos)
              currentState <- ctx.state.get
              queue        = currentState.nodeQueue
              objectSize   = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
              up           = Upload(
                operationId = opId,
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
                  val op = up.copy(serialNumber = q.length+completed.length,nodeId = n)
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

  def distributionSchema(completedOperations:List[CompletedOperation]): Map[String, List[String]] = {
    completedOperations.groupBy(_.objectId).map{
      case (oId,cOps) => oId -> cOps.map(_.nodeId)
    }
  }

  //
  //  def processUploadRequest()(ur: UploadRequest,nodexs:Map[String,NodeX]) = {
//    val xx = ur.what.foldLeft((nodexs,List.empty[ReplicationSchema])) {
//      case (x, w) =>
//        val ns            = x._1
//        val rf            = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)
//        val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
//        val selectedNodes = Operations.uploadBalance(lbToken, ns)(operations = operations, objectSize = objectSize, rf = rf)
//        val xs            = selectedNodes.map(n => Operations.updateNodeX(n, objectSize))
//        val y             = xs.foldLeft(ns) {
//          case (xx, n) => xx.updated(n.nodeId, n)
//        }
//        val yy            = selectedNodes.map(_.nodeId).map(y)
//        val pivotNode     = yy.head
//        val where         = yy.tail.map(_.nodeId)
//        val rs            = ReplicationSchema(
//          nodes = Nil,
//          data = Map(pivotNode.nodeId -> ReplicationProcess(what = w::Nil,where =where,how = How("ACTIVE","PUSH"),when = "REACTIVE" ))
//        )
//        ( y, x._2 :+ rs )
//    }
//    xx
//  }
}
