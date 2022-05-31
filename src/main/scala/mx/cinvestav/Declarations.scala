package mx.cinvestav
import cats.Order
import cats.data.NonEmptyList
import cats.effect.std.Semaphore
import cats.effect.{FiberIO, IO, Ref}
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.types.{CompletedOperation, Download, NodeX, Operation, PendingReplication, Upload, UploadHeaders}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, Evicted, Get, GetCompleted, Missed, ObjectHashing, Push, Put, PutCompleted, RemovedNode, Replicated, UpdatedNodePort, Uploaded, Pull => PullEvent, TransferredTemperature => SetDownloads}
import mx.cinvestav.events.Events.{GetInProgress, HotObject, MeasuredServiceTime, MonitoringStats, UpdatedNetworkCfg}
import org.http4s.Headers
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.typelevel.ci.CIString
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.typelevel.log4cats.Logger
//
import fs2.concurrent.SignallingRef
import cats.effect.std.Queue

import java.io.ByteArrayOutputStream
import java.util.UUID
import mx.cinvestav.commons.types.Monitoring

object Declarations {

  object Implicits {
    implicit val nodeXOrder: Order[NodeX] = (x: NodeX, y: NodeX) => Order.compare[Int](x.nodeId.hashCode, y.nodeId.hashCode)

    implicit val eventDecoderX:Decoder[EventX] = (hCursor:HCursor) =>{
      for {
        eventType <- hCursor.get[String]("eventType")
        decoded   <- eventType match {
          case "UPLOADED" => hCursor.as[Uploaded]
          case "EVICTED" => hCursor.as[Evicted]
          case "DOWNLOADED" => hCursor.as[Downloaded]
          case "PUT" => hCursor.as[Put]
          case "GET" => hCursor.as[Get]
          case "DEL" => hCursor.as[Del]
          case "PUSH" => hCursor.as[Push]
          case "PULL" => hCursor.as[PullEvent]
          case "MISSED" => hCursor.as[Missed]
          case "ADDED_NODE" => hCursor.as[AddedNode]
          case "REMOVED_NODE" => hCursor.as[RemovedNode]
          case "REPLICATED" => hCursor.as[Replicated]
          case "SET_DOWNLOADS" => hCursor.as[SetDownloads]
          case "GET_IN_PROGRESS" => hCursor.as[GetInProgress]
          case "OBJECT_HASHING" => hCursor.as[ObjectHashing]
          case "UPDATED_NODE_PORT" => hCursor.as[UpdatedNodePort]
          case "MEASURED_SERVICE_TIME" => hCursor.as[MeasuredServiceTime]
          case "HOT_OBJECT" => hCursor.as[HotObject]
          case "MONITORING_STATS" => hCursor.as[MonitoringStats]
          case "UPDATED_PUBLIC_PORT" => hCursor.as[UpdatedNetworkCfg]
          case "PUT_COMPLETED" => hCursor.as[PutCompleted]
          case "GET_COMPLETED" => hCursor.as[GetCompleted]

        }
      } yield decoded
    }
    implicit val eventXEncoder: Encoder[EventX] = {
      case p: Put => p.asJson
      case g: Get => g.asJson
      case d: Del => d.asJson
      case push:Push => push.asJson
      case pull:PullEvent => pull.asJson
      case x: Uploaded => x.asJson
      case y: Downloaded => y.asJson
      case y: AddedNode => y.asJson
      case rmn: RemovedNode => rmn.asJson
      case x:Evicted => x.asJson
      case r: Replicated => r.asJson
      case m: Missed => m.asJson
      case sd:SetDownloads => sd.asJson
      case sd:GetInProgress => sd.asJson
      case sd:ObjectHashing => sd.asJson
      case sd:UpdatedNodePort => sd.asJson
      case sd:MeasuredServiceTime => sd.asJson
      case hot:HotObject => hot.asJson
      case hot:MonitoringStats => hot.asJson
      case hot:UpdatedNetworkCfg => hot.asJson
      case hot:PutCompleted => hot.asJson
      case hot:GetCompleted => hot.asJson

    }
    implicit val operationEncoder:Encoder[Operation] = {
      case d: Download => d.asJson
      case u: Upload => u.asJson
      case _ => Json.Null
    }
  }


  case class PendingSystemReplica(rf:Int,ar:Int,mandatory:Boolean = false)

  case class User(id:String,bucketName:String)

  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           serviceTimeNanos:Long,
                           timestamp:Long,
                           level:Int
                         )
  case class ReplicationResponse(guid:String,replicas:List[String],milliSeconds:Long,timestamp:Long,rf:Int=1)
  case class ObjectNodeKey(objectId:String,nodeId:String)
  case class ObjectId(value:String){
    def toObjectNodeKey(nodeId:String): ObjectNodeKey = ObjectNodeKey(value,nodeId)
  }

  case class CreateNodeResponse(
                                 nodeId:String,
                                 url:String,
                                 ip:String,
                                 port:Int,
                                 dockerPort:Int,
                                 milliSeconds:Long
                               )



  case class NodeState(
                        downloadBalancerToken:String="ROUND_ROBIN",
                        uploadBalancerToken:String="ROUND_ROBIN",
                        events:List[EventX] = Nil,
                        pendingReplicas:Map[String,PendingReplication] = Map.empty[String,PendingReplication],
                        pendingSystemReplicas:List[PendingSystemReplica] = Nil,
                        misses:Map[String,Int] = Map.empty[String,Int],
                        replicationFactor:Int = 0,
                        availableResources:Int = 5,
                        replicationTechnique:String = "ACTIVE",
                        pendingQueue:Map[String,Option[Operation]] = Map.empty[String,Option[Operation]],
                        nodeQueue:Map[String,List[Operation] ] = Map.empty[String,List[Operation]],
                        completedQueue:Map[String,List[CompletedOperation]] = Map.empty[String,List[CompletedOperation]],
                        operations:List[Operation] = Nil,
                        completedOperations:List[CompletedOperation] = Nil,
                        lastSerialNumber:Int =0,
                        nodes:Map[String,NodeX] = Map.empty[String,NodeX]

                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            errorLogger: Logger[IO],
                            state:Ref[IO,NodeState],
                            client:Client[IO],
                            systemReplicationSignal:SignallingRef[IO,Boolean]
                          )

  object UploadHeadersOps {
    def fromHeaders(headers:Headers)(implicit ctx:NodeContext) = {
      for {
        serviceTimeStart     <- IO.monotonic.map(_.toNanos)
        //     ________________________________________________________________
        operationId             = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        clientId                = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        objectId                = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectSize              = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        fileExtension           = headers.get(CIString("File-Extension")).map(_.head.value).getOrElse("")
        filePath                = headers.get(CIString("File-Path")).map(_.head.value).getOrElse(s"$objectId.$fileExtension")
        compressionAlgorithm    = headers.get(CIString("Compression-Algorithm")).map(_.head.value).getOrElse("")
        requestStartAt          = headers.get(CIString("Request-Start-At")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
        catalogId               = headers.get(CIString("Catalog-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        digest                  = headers.get(CIString("Digest")).map(_.head.value).getOrElse("DIGEST")
        blockIndex              = headers.get(CIString("Block-Index")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        blockTotal              = headers.get(CIString("Block-Total")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        arrivalTime             = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
        collaborative           = headers.get(CIString("Collaborative")).map(_.head.value).flatMap(_.toBooleanOption).getOrElse(false)
        replicationTechnique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        replicationTransferType = headers.get(CIString("Replication-Transfer-Type")).map(_.head.value).getOrElse(ctx.config.replicationTransferType)
        replicaNodes            = headers.get(CIString("Replica-Node")).map(_.map(_.value).toList).getOrElse(Nil)
        pivotReplicaNode        = headers.get(CIString("Pivot-Replica-Node")).map(_.head.value).getOrElse("PIVOT_REPLICA_NODE")
        replicationFactor       = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        _blockId                = s"${objectId}_${blockIndex}"
        blockId                 = headers.get(CIString("Block-Id")).map(_.head.value).getOrElse(_blockId)
//      __________________________________________________________________________________________________________________
        upheaders               = UploadHeaders(
          operationId             = operationId,
          objectId                = objectId,
          objectSize              = objectSize,
          fileExtension           = fileExtension,
          filePath                = filePath,
          compressionAlgorithm    = compressionAlgorithm,
          requestStartAt          = requestStartAt,
          catalogId               = catalogId,
          digest                  = digest,
          blockIndex              = blockIndex,
          blockTotal              = blockTotal,
          arrivalTime             = arrivalTime,
          collaborative           = collaborative,
          replicaNodes            = replicaNodes,
          replicationTechnique    = replicationTechnique,
          replicationTransferType = replicationTransferType,
          blockId                 = blockId,
          clientId                = clientId,
          pivotReplicaNode        = pivotReplicaNode,
          replicationFactor       = replicationFactor,
          correlationId           = ""
        )
      } yield upheaders
    }
  }
}
