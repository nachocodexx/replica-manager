package mx.cinvestav

import cats.{Id, Order}
import cats.data.NonEmptyList
import cats.effect.std.Semaphore
import cats.effect.{FiberIO, IO, Ref}
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.{DefaultConfig, NodeInfo}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.commons.balancer.v3.{Balancer => BalancerV3}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, Evicted, Get, Missed, ObjectHashing, Push, Put, RemovedNode, Replicated, UpdatedNodePort, Uploaded, Pull => PullEvent, TransferredTemperature => SetDownloads}
import mx.cinvestav.events.Events.{GetInProgress, HotObject, MeasuredServiceTime, MonitoringStats}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
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

    }
  }


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
//  case class NodeX(nodeId:String, ip:String, port:Int, metadata:Map[String,String]= Map.empty[String,String]){
//    def httpUrl = s"http://$ip:$port"
//  }


//  case class SchemaMap(node:String,downloadCounter:Int)
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
                        status:Status,
                        ip:String,
                        lb:Option[Balancer[NodeX]]=None,
                        uploadBalancer:Option[BalancerV3]=None,
                        downloadBalancer:Option[BalancerV3]=None,
                        downloadBalancerToken:String="ROUND_ROBIN",
                        extraDownloadBalancer:Option[BalancerV3]= None,
                        events:List[EventX] = Nil,
                        monitoringEvents:List[EventX]= Nil,
                        monitoringEx:Map[String,EventX]= Map.empty[String,EventX],
//                        systemRepSignal:SignallingRef[IO,Boolean],
                        systemSemaphore:Semaphore[IO],
                        serviceReplicationDaemon:Boolean,
                        serviceReplicationThreshold:Double,
                        maxAR:Int,
                        maxRF:Int,
                        balanceTemperature:Boolean,
                        replicationDaemon:Boolean,
                        replicationDaemonDelayMillis:Long,
                        replicationStrategy:String,
                        experimentId:String,
                        replicationDaemonSingal:SignallingRef[IO,Boolean],
                        infos:List[Monitoring.NodeInfo] = Nil
                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            errorLogger: Logger[IO],
                            state:Ref[IO,NodeState],
                            client:Client[IO]
//                            rabbitMQContext:Option[RabbitMQContext]
                          )
}
