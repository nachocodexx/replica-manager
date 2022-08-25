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
import scala.concurrent.duration._
import language.postfixOps
import fs2._
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
//      "sn-2" -> NodeX(nodeId = "sn-2", ip = "", port = 0,
//        totalStorageCapacity = 100,
//        availableStorageCapacity = 100,
//        usedStorageCapacity = 0,
//        metadata = Map.empty[String, String],
//        ufs = NodeUFs.empty("sn-2"),
//        totalMemoryCapacity = 100,
//        availableMemoryCapacity = 100,
//        usedMemoryCapacity = 0),
//      "sn-3" -> NodeX(nodeId = "sn-3", ip = "", port = 0,
//        totalStorageCapacity = 100,
//        availableStorageCapacity = 100,
//        usedStorageCapacity = 0,
//        metadata = Map.empty[String, String],
//        ufs = NodeUFs.empty("sn-3"),
//        totalMemoryCapacity = 100,
//        availableMemoryCapacity = 100,
//        usedMemoryCapacity = 0),
//      "sn-4" -> NodeX(nodeId = "sn-4", ip = "", port = 0,
//        totalStorageCapacity = 100,
//        availableStorageCapacity = 100,
//        usedStorageCapacity = 0,
//        metadata = Map.empty[String, String],
//        ufs = NodeUFs.empty("sn-4"),
//        totalMemoryCapacity = 100,
//        availableMemoryCapacity = 100,
//        usedMemoryCapacity = 0)

    )
//  ________________________________________________________________
    for {
      _                           <- IO.unit
      defaultState                = NodeState(
          nodes =nodexs,
          operations = List(
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-0",objectSize = 10),
            Upload.empty.copy(nodeId = "sn-1",objectSize=10),
            Upload.empty.copy(nodeId = "sn-1",objectSize=10),
            Upload.empty.copy(nodeId = "sn-1",objectSize=10),
          ),
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
      what                        = List(
          What(id = "f1", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "2")  ),
          What(id = "f2", url = "", metadata = Map("OBJECT_SIZE"->"10", "REPLICATION_FACTOR"-> "1")  ),
      )
      ur                          = UploadRequest(
        what                 = what,
        elastic              = true,
        replicationTechnique = "PASSIVE",
        metadata             = Map.empty[String,String]
      )
//      (newNodexs,rss)             =  Operations.processUploadRequest(
      pur             =  Operations.processUploadRequest(
        lbToken= "MIN_WAITING_TIME",
        operations =defaultState.operations,
        queue = defaultState.nodeQueue,
        completedQueue = defaultState.completedQueue,
      )(ur = ur,nodexs = defaultState.nodes)
      _                           <- ctx.state.update(s=>s.copy(nodes = pur.nodexs ))
      clientId                    = "client-0"
//    ____________________________________
      processRS       = (sr:ReplicationSchema) => Operations.processRSAndUpdateQueue(clientId = clientId)(rs = sr )
      xs              <- pur.rss.traverse(processRS).map(_.flatten)
      xsGrouped       = xs.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }
      x               <- Operations.generateUploadBalance(xs = xsGrouped)
//      _ <- IO.println(pur.asJson.toString)
//    ____________________________________
      currentState <- ctx.state.get
      _ <- IO.println(currentState.nodes.toString)
//      _ <- fx
      newN = Operations.processNodes(
        nodexs = currentState.nodes,
        completedOperations = currentState.completedOperations,
        operations= currentState.operations,
        queue = currentState.nodeQueue
      )
      _ <- IO.println(newN.toMap.asJson.toString)
    } yield ()
  }
  test("A"){
    val baseUpCompletedSn0 = UploadCompleted.empty.copy(serviceTime = 1000,nodeId ="sn-0")
    val baseUpCompletedSn1 = UploadCompleted.empty.copy(serviceTime = 1000,nodeId ="sn-1")
    val queue = Map("sn-0" -> List(
      Upload.empty.copy(arrivalTime = 0),
      Upload.empty.copy(arrivalTime = 100)
    ),
      "sn-1"->List.fill(10)(Upload.empty.copy(arrivalTime=0))
    )
    val completedOps = Map("sn-0"->List.fill(100)(baseUpCompletedSn0), "sn-1"-> List.fill(10)(baseUpCompletedSn1))
    val wts = Operations.getAVGWaitingTimeByNode(completedOperations = completedOps,queue = queue)
    println(wts)
  }

  test("R") {
    for {
      ref <- IO.ref(false)
      res <- Stream.awakeEvery[IO](1 second)
        .evalMap{ x =>
          for{
            _   <- IO.println(s"ATTEMP $x")
            res <- ref.get
            _   <- if(res) IO.println("END") *> IO.canceled else IO.unit
          } yield ()
        }
        .compile.drain.start
      _ <- IO.sleep(5 seconds) *> ref.update(_=>true)
      _ <- IO.println(res.toString)
      _ <- IO.sleep(5 seconds)
    } yield ()
  }

  test("L"){
    val x = Map("f0"->Map("sn-0"->2,"sn-1"->1))
    val y = Map("f0"-> Map("sn-0"->0,"sn-2"->0),"f1"-> Map("sn-0"->0,"sn1"->0) )
    val z = x |+| y
    println(z)
//    val x = Map("a"->List("b"))
//    val y = x |+| Map("a"-> List("c"))
//    println(y)
  }

}
