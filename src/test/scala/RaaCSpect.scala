import mx.cinvestav.commons.types.{How, NodeQueueStats, NodeUFs, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadBalance, UploadCompleted, UploadRequest, UploadResult, What}
import mx.cinvestav.operations.Operations
import mx.cinvestav.commons.{types, utils}
import cats.implicits._
import cats.effect._
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{NodeContext, NodeState}
import mx.cinvestav.config.DefaultConfig
import org.http4s.blaze.client.BlazeClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Declarations.Implicits._
class RaaCSpect extends munit .CatsEffectSuite {

  test("K") {
    val nodexs = Map(
      "sn-0" -> NodeX(nodeId = "sn-0", ip = "", port = 0,
        totalStorageCapacity = 100,
        availableStorageCapacity = 100,
        usedStorageCapacity = 0,
        metadata = Map.empty[String, String],
        ufs = NodeUFs.empty("sn-0"),
        totalMemoryCapacity = 100,
        availableMemoryCapacity = 100,
        usedMemoryCapacity = 0),
      "sn-1" -> NodeX(nodeId = "sn-1", ip = "", port = 0,
        totalStorageCapacity = 100,
        availableStorageCapacity = 100,
        usedStorageCapacity = 0,
        metadata = Map.empty[String, String],
        ufs = NodeUFs.empty("sn-1"),
        totalMemoryCapacity = 100,
        availableMemoryCapacity = 100,
        usedMemoryCapacity = 0),
      "sn-2" -> NodeX(nodeId = "sn-2", ip = "", port = 0,
        totalStorageCapacity = 100,
        availableStorageCapacity = 100,
        usedStorageCapacity = 0,
        metadata = Map.empty[String, String],
        ufs = NodeUFs.empty("sn-2"),
        totalMemoryCapacity = 100,
        availableMemoryCapacity = 100,
        usedMemoryCapacity = 0),
      "sn-3" -> NodeX(nodeId = "sn-3", ip = "", port = 0,
        totalStorageCapacity = 100,
        availableStorageCapacity = 100,
        usedStorageCapacity = 0,
        metadata = Map.empty[String, String],
        ufs = NodeUFs.empty("sn-3"),
        totalMemoryCapacity = 100,
        availableMemoryCapacity = 100,
        usedMemoryCapacity = 0)

    )
//    val operations = List(
//      Upload(operationId = "up-0", serialNumber = 0, arrivalTime = 0, objectId = "f1", objectSize = 10, clientId = "", nodeId = "sn-0", metadata = Map.empty[String, String])
//    )
//  ________________________________________________________________
    def fn(ur: UploadRequest,operations:List[Operation]) = {
      val xx = ur.what.foldLeft((nodexs,List.empty[ReplicationSchema] )) {
        case (x, w) =>
          val ns = x._1
          val rf = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)

          val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
          val selectedNodes = Operations.uploadBalance("SORTING_UF", ns)(operations = operations, objectSize = objectSize, rf = rf)
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
    //  }
    val ur0 = UploadRequest(
      what = List(
        What(id = "f1", url = "", metadata = Map("REPLICATION_FACTOR" -> "2", "OBJECT_SIZE" -> "10")),
        What(id = "f2", url = "", metadata = Map("REPLICATION_FACTOR" -> "2", "OBJECT_SIZE" -> "10")),
      ),
      elastic = false
    )



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
               NodeQueueStats(operationId =x.operationId , nodeId = x.nodeId,
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

    def processRS(clientId:String)(rs:ReplicationSchema )(implicit ctx:NodeContext) = {

      rs.data.toList.traverse{
        case (nodeId, rp) =>
          for {
            _              <- IO.unit
            what           = rp.what
            where          = rp.where
            whereCompleted = where :+ nodeId
            operations     <- what.traverse{ w=>
              val correlationId     = utils.generateNodeId(prefix = "op",len=10,autoId = true)
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
                  metadata = Map.empty[String,String],
                  correlationId = correlationId
                )
                ops          = whereCompleted.foldLeft( (queue,List.empty[Operation]) ){
                  case (x,n)=>
                    val queues = x._1
                    val q = queues.getOrElse(n,Nil)
                    val completed = currentState.completedQueue.getOrElse(n,Nil)
                    val opId     = utils.generateNodeId(prefix = "op",len=10,autoId = true)
                    val op = up.copy(serialNumber = q.length+completed.length,nodeId = n,operationId = opId)
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

//    val res = fn(ur0)


    for {
      _           <- IO.unit
      state       <-  IO.ref(NodeState(
        completedQueue = Map(
          "sn-0"-> List(
            UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 0,nodeId = "sn-0"),
            UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 0,nodeId = "sn-0"),
            UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 0,nodeId= "sn-0"),
          )
        ),completedOperations = List(
          UploadCompleted.empty.copy(serviceTime = 30,waitingTime = 0,nodeId = "sn-0"),
          UploadCompleted.empty.copy(serviceTime = 50,waitingTime = 0,nodeId = "sn-0"),
          UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 0,nodeId= "sn-0"),
        )
      ))
      (client,fx) <- BlazeClientBuilder[IO](executionContext = ExecutionContext.global).resource.allocated
      signal      <- SignallingRef[IO,Boolean](false)
//    __________________________________________________________________
      implicit0(ctx:NodeContext)         = NodeContext(
        config                  = DefaultConfig(),
        logger                  = Slf4jLogger.getLogger[IO],
        errorLogger             = Slf4jLogger.getLogger[IO],
        state                   = state,
        client                  = client,
        systemReplicationSignal = signal
      )
      ur          = UploadRequest(
        what = List(
          What(id = "f1", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "3")  ),
          What(id = "f2", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "3")  )
        ),
        elastic = true
      )
      x            = fn(ur = ur,operations = Nil)
      clientId     = "client-0"
//    ____________________________________
      xs           <- x._2.traverse(processRS(clientId)).map(_.flatten)
      xsGrouped    = xs.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }

      x <- generateUploadBalance(xs = xsGrouped)
      _            <- IO.println(x.asJson.toString)
//      _            <- IO.println(xsGrouped.asJson.toString)
      
//    ____________________________________
      currentState <- ctx.state.get
//      _            <- IO.println(currentState.nodeQueue.asJson.toString)
    } yield ()
  }

}
