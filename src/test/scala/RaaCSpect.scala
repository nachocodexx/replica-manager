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
//import mx.cinvestav.operations.Operations
class RaaCSpect extends munit .CatsEffectSuite {

  test("AA") {
    val x = Map("a"->2,"b"-> 0)
    val y = Map("a"->1,"b"->1)
    val z = x++y
    println(z)
  }
  test("Distribution Schema"){
    val ops = List(
      Upload.empty
    )
    val cOps = List(UploadCompleted.empty)
    val queue = Map(
      "sn-0" -> List(
        Upload.empty
      )
    )
    val res = Operations.distributionSchema(operations = ops,completedOperations = cOps,queue = queue)
  }

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
        usedMemoryCapacity = 0),
      "sn-4" -> NodeX(nodeId = "sn-4", ip = "", port = 0,
        totalStorageCapacity = 100,
        availableStorageCapacity = 100,
        usedStorageCapacity = 0,
        metadata = Map.empty[String, String],
        ufs = NodeUFs.empty("sn-4"),
        totalMemoryCapacity = 100,
        availableMemoryCapacity = 100,
        usedMemoryCapacity = 0)

    )
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
          val where         = yy.tail.map(_.nodeId).toList
          val rs = ReplicationSchema(
            nodes = Nil,
            data = Map(pivotNode.nodeId -> ReplicationProcess(what = w::Nil,where =where,how = How("ACTIVE","PUSH"),when = "REACTIVE" ))
          )
          ( y, x._2 :+ rs )
      }
      xx
    }

    for {
      _                           <- IO.unit
      defaultState                = NodeState(
          nodes =nodexs,
          operations = List(),
          completedQueue = Map(
            "sn-0"-> List(
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 1000,nodeId = "sn-0"),
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 1000,nodeId = "sn-0"),
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime =530 ,nodeId= "sn-0"),
            ),
            "sn-1"-> List(
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 1000,nodeId = "sn-1"),
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 1000,nodeId = "sn-1"),
              UploadCompleted.empty.copy(serviceTime = 100,waitingTime =530 ,nodeId= "sn-1"),
            ),
      ),
      completedOperations = List(
            UploadCompleted.empty.copy(serviceTime = 30,waitingTime = 0,nodeId = "sn-0"),
            UploadCompleted.empty.copy(serviceTime = 50,waitingTime = 0,nodeId = "sn-0"),
            UploadCompleted.empty.copy(serviceTime = 100,waitingTime = 0,nodeId= "sn-0"),
          )
      )
      state                       <-  IO.ref(defaultState)
      (client,fx)                 <- BlazeClientBuilder[IO](executionContext = ExecutionContext.global).resource.allocated
      signal                      <- SignallingRef[IO,Boolean](false)
//    __________________________________________________________________
      implicit0(ctx:NodeContext)  = NodeContext(
        config                  = DefaultConfig(),
        logger                  = Slf4jLogger.getLogger[IO],
        errorLogger             = Slf4jLogger.getLogger[IO],
        state                   = state,
        client                  = client,
        systemReplicationSignal = signal
      )
      ur                          = UploadRequest(
        what = List(
          What(id = "f1", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "3")  ),
//          What(id = "f2", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "3")  )
        ),
        elastic = true
      )


      (newNodexs,rss)             =  Operations.processUploadRequest(
        lbToken= "MIN_WAITING_TIME",
        operations =defaultState.operations,
        queue = defaultState.nodeQueue,
        completedQueue = defaultState.completedQueue,
      )(ur = ur,nodexs = defaultState.nodes)
      _                           <- ctx.state.update(s=>s.copy(nodes = newNodexs))
      clientId                    = "client-0"
//    ____________________________________
      processRS       = (sr:ReplicationSchema) => Operations.processRSAndUpdateQueue(clientId = clientId)(rs = sr )
      xs              <- rss.traverse(processRS).map(_.flatten)
      xsGrouped       = xs.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }
      x               <- Operations.generateUploadBalance(xs = xsGrouped)
      _               <- IO.println(newNodexs.asJson.toString)
//    ____________________________________
      currentState <- ctx.state.get
      _ <- fx
    } yield ()
  }

}
