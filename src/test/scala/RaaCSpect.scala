import mx.cinvestav.commons.types.{How, NodeUFs, NodeX, ReplicationProcess, ReplicationSchema, Upload, UploadRequest, What}
import mx.cinvestav.operations.Operations
import mx.cinvestav.commons.utils
import cats.implicits._
import cats.effect._
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{NodeContext, NodeState}
import mx.cinvestav.config.DefaultConfig
import org.http4s.blaze.client.BlazeClientBuilder

import java.lang.System.Logger
import scala.concurrent.ExecutionContext
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
    val operations = List(
      Upload(operationId = "up-0", serialNumber = 0, arrivalTime = 0, objectId = "f1", objectSize = 10, clientId = "", nodeId = "sn-0", metadata = Map.empty[String, String])
    )
    val fn =
      (ur: UploadRequest) => {
      val xx = ur.what.foldLeft(
        (nodexs,List.empty[ReplicationSchema])
      ) {
        case (x, w) =>
          val ns = x._1
          val rf = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(1)

          val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
          val selectedNodes = Operations.uploadBalance("SORTING_UF", ns)(operations = operations, objectSize = objectSize, rf = rf)
          val xs            = selectedNodes.map(n => Operations.updateNodeX(n, objectSize))
          val y             = xs.foldLeft(ns) {
            case (xx, n) => xx.updated(n.nodeId, n)
          }
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

    def processRS (rs:ReplicationSchema )(implicit ctx:NodeContext) = {
      val xs       = rs.data.flatMap(_._2.where).toList
      val nodesIds = (rs.data.keys.toList ++ xs ).distinct

      rs.data.toList.traverse{
        case (nodeId, rp) =>
          for {
            _              <- IO.unit
            what           = rp.what
            where          = rp.where
            whereCompleted = where :+ nodeId
            operations     <- what.traverse{ w=>
              val opId = utils.generateNodeId(prefix = "op",autoId = true)
              for {
                arrivalTime  <- IO.monotonic.map(_.toNanos)
                currentState <- ctx.state.get
                objectSize   = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
                up           = Upload(
                  operationId = opId,
                  serialNumber = 0,
                  arrivalTime =arrivalTime ,
                  objectId = w.id,
                  objectSize = objectSize,
                  clientId = "",
                  nodeId = "",
                  metadata = Map.empty[String,String]
                )
                ops          = whereCompleted.foldLeft(currentState.nodeQueue){
                  case (queues,n)=>
                    val q = queues.getOrElse(n,Nil)
                    queues.updated(n,q :+ up.copy(serialNumber = q.length,nodeId = n))
                }
              } yield ()
            }

          } yield ()
      }
//      nodesIds.traverse{ nId =>
//        for {
//          maybeRp = rs.data.get(nId)
//          x       = maybeRp match {
//            case Some(rp) =>
//              rp.what.map { w =>
//                Upload(
//                  operationId = opId,
//                  serialNumber = ???,
//                  arrivalTime = ???,
//                  objectId = ???,
//                  objectSize = ???,
//                  clientId = ???,
//                  nodeId = ???,
//                  metadata = ???
//                )
//              }
//            case None => Nil
//          }
////          up =  Upload(operationId =opId, serialNumber = 0, arrivalTime = arrivalTime, objectId = ???, objectSize = ???, clientId = ???, nodeId = ???, metadata = ???)
//        } yield ()
//      }
    }

    val res = fn(ur0)


    for {
      _ <- IO.unit
      state <-  IO.ref(NodeState())
      ctx = NodeContext(
        config = DefaultConfig(),
        logger = Logger[IO],
        errorLogger = Logger[IO],
        state =state,
        client                     = BlazeClientBuilder[IO](executionContext = ExecutionContext.global).withDefaultSocketReuseAddress.resource.allocated,
        systemReplicationSignal = SignallingRef[IO](false)
      )
    } yield ()


    //    val fn = (rs:UploadRequest) => {
    //          val xx = rp.what.foldLeft(nodexs){
    //            case (ns,w )=>
    //              val rf            = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption).getOrElse(0)
    //
    //              val objectSize    = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
    //              val pivotNode     = ns(nodeId)
    //              val nodesWithoutPivot = ns.filter(_._1 != nodeId)
    //              val selectedNodes = Operations.uploadBalance("SORTING_UF",nodesWithoutPivot)(operations = operations,objectSize= objectSize,rf=rf):+ pivotNode
    //              val xs            = selectedNodes.map(n=> Operations.updateNodeX(n,objectSize))
    //              val y             = xs.foldLeft(ns){
    //                case (xx,n ) =>xx.updated(n.nodeId,n)
    //              }
    //              y
    //          }
    //          xx
    //      }
    //  }
  }

}
