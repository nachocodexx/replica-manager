package mx.cinvestav.operations

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.types
import mx.cinvestav.commons.types.{NodeQueueStats, NodeReplicationSchema, UploadBalance, UploadResult}
import org.http4s.dsl.io._
//{InternalServerError,Forbidden}
//import org.http4s.dsl.io.NotFound
import org.http4s.{Header, Headers, Method, Request, Response, Status, Uri}
import org.typelevel.ci.CIString
import scala.util.Random
import scala.concurrent.duration._
import language.postfixOps
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.types.{Download, DownloadCompleted, How, NodeUFs, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadCompleted, UploadRequest,CompletedOperation}
import mx.cinvestav.commons.balancer.nondeterministic
import mx.cinvestav.commons.utils
//
import retry._
import retry.implicits._

object Operations {

  def ballAccess(completedOperations:List[CompletedOperation]): Map[String, Int] = {
    onlyDownloadCompleted(completedOperations).asInstanceOf[List[DownloadCompleted]]
      .groupBy(_.objectId)
      .map{
        case (objectId, ops) => objectId -> ops.length
      }
  }

  def download(d:Download)(implicit ctx:NodeContext) = {
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(s"http://${d.nodeId}:6666/api/v3/download/${d.objectId}"),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"), d.operationId),
        Header.Raw(CIString("Client-Id"), d.clientId),
        Header.Raw(CIString("Object-Size"), d.objectSize.toString),
        Header.Raw(CIString("Serial-Number"), d.serialNumber.toString),
      )
    )
    for {
      response <- ctx.client.stream(req = req).flatMap(_.body).compile.to(Array)
      _        <- ctx.logger.debug(response.toString)
    } yield response
  }

  def downloadv2(d:Download)(implicit ctx:NodeContext) = {
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(s"http://${d.nodeId}:6666/api/v3/download/${d.objectId}"),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"), d.operationId),
        Header.Raw(CIString("Client-Id"), d.clientId),
        Header.Raw(CIString("Object-Size"), d.objectSize.toString),
        Header.Raw(CIString("Serial-Number"), d.serialNumber.toString),
      )
    )
    ctx.client.stream(req)
  }

  def launchOperation(op:Operation)(implicit ctx:NodeContext) = {
    val x = op match {
      case d: Download =>
        val storageNodeURL = s"http://${d.nodeId}:6666/api/v3/download/${d.objectId}"
        val req = Request[IO](
          method = Method.POST,
          uri = Uri.unsafeFromString(s"http://${d.clientId}:9001/api/v2/pull"),
          headers = Headers(
            Header.Raw(CIString("Object-Id"), d.objectId),
            Header.Raw(CIString("Object-Size"), d.objectSize.toString),
            Header.Raw(CIString("Operation-Id"), d.operationId),
            Header.Raw(CIString("Serial-Number"), d.serialNumber.toString),
            Header.Raw(CIString("Object-Uri"), storageNodeURL),
          )
        )
        for {
          response <- ctx.client.stream(req = req).compile.lastOrError.handleErrorWith{ e=>
            val x = ctx.logger.error(e.getMessage)  *> InternalServerError()
            x
          }
          _        <- ctx.logger.debug("NODE_UPLOAD_STATUS "+response.toString)
        } yield response

      case u: Upload =>
        val req = Request[IO](
          method = Method.POST,
          uri = Uri.unsafeFromString(s"http://${u.nodeId}:6666/api/v3/upload"),
          headers = Headers(
            Header.Raw(CIString("Object-Id"), u.objectId),
            Header.Raw(CIString("Operation-Id"), u.operationId),
            Header.Raw(CIString("Client-Id"), u.clientId),
            Header.Raw(CIString("Object-Size"), u.objectSize.toString),
            Header.Raw(CIString("Object-Uri"), u.metadata.getOrElse("URL","")),
            Header.Raw(CIString("Serial-Number"), u.serialNumber.toString),
          )
        )
        for {
          response <- ctx.client.stream(req = req).compile.lastOrError.handleErrorWith{ e=>
           val x = ctx.logger.error(e.getMessage)  *> InternalServerError()
            x
          }
          _        <- ctx.logger.debug("NODE_UPLOAD_STATUS "+response.toString)
        } yield response
//      case _ => NotFound().pure[IO]
    }
    x
  }

  def nextOperation(nodexs:List[NodeX],queue:Map[String,List[Operation]],pending:Map[String,Option[Operation]])(implicit ctx:NodeContext) ={
    val (newPending, nextOps)  = nodexs.foldLeft( (pending,List.empty[ Operation ]) ){
      case ( p,node ) =>
        val  nodeId = node.nodeId
        val pendingOp      = pending.getOrElse(nodeId,None)
        val q       = queue.getOrElse(nodeId,Nil).sortBy(_.serialNumber)
        println(s"QUEUE $q")
        println(s"PENDING_OP $pendingOp")
        pendingOp match {
        case Some(_) => p
        case None =>
          val nextOperation = q.headOption
//          println(nextOperation)
          nextOperation match {
            case Some(value) =>
              (p._1.updated(nodeId,nextOperation), p._2:+ value)
            case None => (p._1.updated(nodeId,nextOperation),p._2 )
          }
      }
    }

    for {
      _    <- ctx.logger.debug("NEXT_OPERATION: "+nextOps.toString)
      _    <- ctx.state.update{s=>s.copy( pendingQueue = newPending )}
      reqs <-  nextOps.traverse { op =>
        launchOperation(op).retryingOnFailures(
                  wasSuccessful = (res:Response[IO]) => (res.status.code == 204) .pure[IO],
                  policy = RetryPolicies.limitRetries[IO](
                    maxRetries = ctx.config.maxRetries
                  ) join RetryPolicies.exponentialBackoff(ctx.config.exponentialBackoffMs milliseconds),
                  onFailure = (response:Response[IO],rd:RetryDetails) =>
                    ctx.logger.error(s"RETRY ${op.operationId}")
                )
      }
    } yield ()
  }

  def onlyUpload(operations:List[Operation]): List[Operation] = operations.filter {
    case _:Upload => true
    case _ => false
  }
// _____________________________________________________________________________________________________________________
  def onlyDownload(operations:List[Operation]): List[Operation] = operations.filter {
    case _:Download => true
    case _ => false
  }
  def onlyDownloadCompleted(operations:List[CompletedOperation]) = operations.filter {
    case _:DownloadCompleted => true
    case _ => false
  }
// _____________________________________________________________________________________________________________________

  def getAVGWaitingTimeByNode(completedOperations:Map[String,List[CompletedOperation]],queue:Map[String,List[Operation]]): Map[String, Double] ={
    val sts = getAVGServiceTime(operations = completedOperations.values.flatten.toList)
    queue.map{
      case (nodeId, ops) =>
//        val lastDt = completedOperations.getOrElse(nodeId,Nil).maxByOption(_.serialNumber).map(_.departureTime).getOrElse(0L)
        val avgST  = sts.getOrElse(nodeId,0.0)
        val (_,cOps) =  ops.foldLeft( (Option.empty[CompletedOperation],List.empty[CompletedOperation]) ){
          case ((lastOp,cOps),currentOp) =>
            lastOp match {
              case Some(last) =>
                val wt          = last.departureTime - currentOp.arrivalTime
                val completedOp = last.asInstanceOf[UploadCompleted].copy(
                  serviceTime = avgST.toLong,
                  arrivalTime = currentOp.arrivalTime,
                  waitingTime = if(wt < 0 ) 0L else wt,
                  idleTime    =   if(wt<0) wt*(-1) else 0L
                )
                (completedOp.some, cOps:+completedOp)
              case None =>
                val completedOp = UploadCompleted.empty.copy(
                  serviceTime = avgST.toLong,
                  arrivalTime = currentOp.arrivalTime,
                  waitingTime = 0L,
                  idleTime    = 0L
                )
                (completedOp.some,cOps:+ completedOp)
            }
        }
        (nodeId -> (if(cOps.isEmpty) 0.0 else cOps.map(_.waitingTime).sum.toDouble/cOps.length.toDouble) )
    }
  }


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


  def processNodes(nodexs:Map[String,NodeX],
                   completedOperations:List[CompletedOperation],
                   queue:Map[String,List[Operation]],
                   operations:List[Operation] = Nil,
                   objectSize:Long=0L
                  )  = {
    nodexs.map{
      case (nodeId,n) =>
        val ops              = operations.filter(_.nodeId == nodeId)
        val pendingOps =  queue.getOrElse(nodeId,List.empty[Operation])

        val _completedOperations = completedOperations.filter(_.nodeId == nodeId)
        val uploads      = onlyUpload(operations = ops).asInstanceOf[List[Upload]]
        val completedUps = onlyUploadCompleted(completedOperations = _completedOperations)
        val pendingUps = onlyUpload(operations = pendingOps)
//      _______________________________________________________________________________
        val downloads     = onlyDownload(operations = ops).asInstanceOf[List[Download]]
        val completedDown = onlyDownloadCompleted(operations = _completedOperations)
        val pendingDown   = onlyDownload(operations = pendingOps)
//      _______________________________________________________________________________
        val used  = uploads.map(_.objectSize).sum
        val usedD = downloads.map(_.objectSize).sum
        n.copy(
          availableStorageCapacity  = n.totalStorageCapacity - used,
          usedStorageCapacity       = used,
//
          availableMemoryCapacity   = n.totalMemoryCapacity - usedD,
          usedMemoryCapacity        = usedD,
          ufs                       = NodeUFs(
            nodeId   = n.nodeId,
            diskUF   = nondeterministic.utils.calculateUF(total =  n.totalStorageCapacity,used = used,objectSize= objectSize),
            memoryUF = nondeterministic.utils.calculateUF(total =  n.totalMemoryCapacity,used = usedD,objectSize= objectSize),
            cpuUF    = 0.0
          ),
          metadata                  = Map(
              "PENDING_UPLOADS" -> pendingUps.length.toString,
              "PENDING_DOWNLOADS" -> pendingDown.length.toString,
              "COMPLETED_UPLOADS" -> completedUps.length.toString,
              "COMPLETED_DOWNLOADS" -> completedDown.length.toString
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

      case "TWO_CHOICES" =>
        val x = Random.nextInt(nodexs.size)
        val y = Random.nextInt(nodexs.size)
        val nodeX = nodexs.toList(x)._2
        val nodeY = nodexs.toList(y)._2
        val selectedNode = if(nodeX.ufs.diskUF < nodeY.ufs.diskUF) nodeX else nodeY
        selectedNode::Nil
      case "SORTING_UF" =>
        nodexs.values.toList.sortBy(_.ufs.diskUF).take(rf)
        .map(n=> Operations.updateNodeX(n,objectSize = objectSize,downloadDiv = 0L))
    }
  }

  def ballAccessByNodes(completedOperations:Map[String,List[CompletedOperation]]): Map[String, Map[String, Int]] = {
    val COPS       = completedOperations.values.toList.flatten
    val cDownloads = onlyDownloadCompleted(COPS).asInstanceOf[List[DownloadCompleted]]
    val nodesByUps = onlyUploadCompleted(COPS).asInstanceOf[List[UploadCompleted]].groupBy(_.objectId)
      .map{
        case (objectId, ops) => objectId -> ops.map(_.nodeId).distinct
      }
    cDownloads.groupBy(_.objectId).map{
      case (objectId,cops) =>
        objectId -> cops.groupBy(_.nodeId)
    }.map{
      case (objectId,mcops) =>
        val replicaNodes = nodesByUps.getOrElse(objectId,Nil).map(n=>n->0).toMap
        val y = mcops.map{
          case (nodeId,ops) => nodeId-> ops.length
        } |+| replicaNodes
//        val default =
        objectId -> y
    }
  }

  def downloadBalance(x:String,nodexs:Map[String,NodeX])(
                     objectId:String,
    operations:List[Operation] = Nil,
    queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],
    completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]],
    objectSize:Long,
  ) = {
    x match {
      case "LEAST_HITS"=>
        val access          = Operations.ballAccessByNodes(completedOperations = completedQueue)
        val accessByReplica = access.get(objectId)
        accessByReplica match {
          case Some(value) => nodexs(value.minBy(_._2)._1)
          case None => nodexs.toList(Random.nextInt(nodexs.size))._2
        }
//        nodexs(accessByReplica.minBy(_._2))
      case "MIN_WAITING_TIME"  =>
        val defaultWtXNode   = nodexs.keys.toList.map(_ -> 0.0).toMap
        val waitingTimeXNode =  (defaultWtXNode ++ Operations.getAVGWaitingTimeByNode(
          completedOperations = completedQueue,
          queue = queue
        )).toList.minBy(_._2)
        Operations.updateNodeX(nodeX = nodexs(waitingTimeXNode._1),objectSize=objectSize,uploadDiv = 0L)
      case "ROUND_ROBIN" =>
        val grouped  = onlyDownload(operations).asInstanceOf[List[Download]].groupBy(_.nodeId)
        val xs       = grouped.map(x=> x._1 -> x._2.length)
        val total    = xs.values.toList.sum
        val AR       = nodexs.size
        val index    = total%AR
        val orderedNodes = (nodexs.values.toList.sortBy(_.nodeId))
        val selectedNode = orderedNodes(index)
        Operations.updateNodeX(selectedNode,objectSize = objectSize)
      case "SORTING_UF" =>
        Operations.updateNodeX(nodexs.values.toList.minBy(_.ufs.memoryUF),objectSize)
//          .map(n=> Operations.updateNodeX(n,objectSize = objectSize))
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


  case class ProcessedUploadRequest(nodexs:Map[String,NodeX],rss:List[ReplicationSchema],pivotNode:Option[NodeX],nrs:Option[List[NodeReplicationSchema]]=None)
  def processUploadRequest(
                            lbToken:String="SORTING_UF",
                            operations:List[Operation],
                            queue:Map[String,List[Operation]] = Map.empty[String,List[Operation]],
                            completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]]
                          )(ur: UploadRequest,nodexs:Map[String,NodeX]): ProcessedUploadRequest = {

//    val maxRF       = ur.what.map(_.metadata.getOrElse("REPLICATION_FACTOR","1").toInt).max
//    val newNodeIds  = if(maxRF > nodexs.size) (0 until  (maxRF-nodexs.size) ).toList.map(index=>utils.generateStorageNodeId(autoId = true)) else List.empty[String]
//    val newNodes    = newNodeIds.map(id => NodeX.empty(nodeId = id))
//    val newNodesMap = newNodes.map(n=> n.nodeId -> n)
//    val nrss        =  newNodes.map{ n=>
//      NodeReplicationSchema.empty( id = n.nodeId)
//    }.some
    val newNodesMap = Map.empty[String,NodeX]

    val pur = ProcessedUploadRequest(nodexs = nodexs ++ newNodesMap, rss = List.empty[ReplicationSchema],pivotNode = None,nrs = None )

    val xx = ur.what.foldLeft(pur) {
      case (x, w) =>
        val ns = x.nodexs
        val rf            = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)
        val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
        val selectedNodes = Operations.uploadBalance(lbToken, ns)(
          operations = operations,
          objectSize = objectSize,
          rf = rf,
          queue = queue,
          completedQueue = completedQueue
        )
        val y             = selectedNodes.foldLeft(ns) { case (xx, n) => xx.updated(n.nodeId, n)}
        val yy            = selectedNodes.map(_.nodeId).map(y).toList
        val pivotNode     = yy.head
        val where         = yy.tail.map(_.nodeId)
         ur.replicationTechnique match {
           case "PASSIVE" =>
             val xx = where.indices.toList.map{ index=>
               if(index ==0)
                 ReplicationSchema(
                   nodes = Nil,
                   data =
                     Map(
                       pivotNode.nodeId -> ReplicationProcess(what = w::Nil, where = where.head::Nil, how= How.passive(), when= "REACTIVE" )
                     )
                 )
               else ReplicationSchema(
                 nodes = Nil,
                 data = Map(
                   where(index - 1) -> ReplicationProcess(what = w::Nil, where = where(index)::Nil, how = How.passive(), when= "REACTIVE")
                 )
               )
             }
             x.copy(nodexs = y, rss = x.rss ++ xx, pivotNode = pivotNode.some)
//             ( y, x._2 ++ xx,pivotNode)
           case "ACTIVE" =>
             val rs = ReplicationSchema(
               nodes = Nil,
               data = Map(
                 pivotNode.nodeId ->
                   ReplicationProcess(what = w::Nil,where =where,how = How("ACTIVE","PUSH"),
                     when = "REACTIVE" )
               )
             )
             x.copy(nodexs = y, rss = x.rss :+ rs, pivotNode = pivotNode.some)
//             ( y, x._2 :+ rs , pivotNode)
         }
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
                metadata = Map("URL"-> w.url)
              )
              ops          = whereCompleted.foldLeft( ( queue, List.empty[Operation] ) ){
                case (queueAndOperations,nodeId)=>
                  val queues = queueAndOperations._1
                  val q = queues.getOrElse(nodeId,Nil)
                  val completed = currentState.completedQueue.getOrElse(nodeId,Nil)
                  val operationId = utils.generateNodeId(prefix = "op",autoId = true,len = 15)
                  val op = up.copy(serialNumber = q.length+completed.length,nodeId = nodeId,operationId = operationId)
                  (
                    queues.updated(nodeId,q :+ op),
                    queueAndOperations._2:+op
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
      case _ => false
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
      avgWaitingTimeByNode = Operations.getAVGWaitingTimeByNode(completedOperations = currentState.completedQueue,queue = currentState.nodeQueue)
      id                   = utils.generateNodeId(prefix = "ub",len = 10,autoId = true)
      xxs = groupedByOId.map{
        case (objectId, value) =>
          val id     = utils.generateNodeId(prefix = "us",len = 10,autoId = true)
          val nodeq  = value.map{ x=>
            NodeQueueStats(
              operationId =x.operationId ,
              nodeId = x.nodeId,
              avgServiceTime = avgServiceTimeByNode.getOrElse(x.nodeId,0.0),
              avgWaitingTime = avgWaitingTimeByNode.getOrElse(x.nodeId,0.0),
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
