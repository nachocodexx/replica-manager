package mx.cinvestav

import cats.Order
import cats.data.NonEmptyList
import cats.effect.std.Semaphore
import cats.effect.{IO, Ref}
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.commons.balancer.v3.{Balancer => BalancerV3}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, Evicted, Get, Missed, ObjectHashing, Push, Put, RemovedNode, Replicated, UpdatedNodePort, Uploaded, Pull => PullEvent, TransferredTemperature => SetDownloads}
import mx.cinvestav.events.Events.GetInProgress
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.typelevel.log4cats.Logger
//
import fs2.concurrent.SignallingRef

import java.io.ByteArrayOutputStream
import java.util.UUID

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
    }
  }


  case class User(id:UUID,bucketName:String)

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
                        systemRepSignal:SignallingRef[IO,Boolean],
                        systemSemaphore:Semaphore[IO],
                        serviceReplicationDaemon:Boolean,
                        serviceReplicationThreshold:Double,
                        maxAR:Int,
                        maxRF:Int,
                        balanceTemperature:Boolean,
                        replicationDaemon:Boolean,
                        replicationDaemonDelayMillis:Long,

                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            errorLogger: Logger[IO],
                            state:Ref[IO,NodeState],
//                            rabbitMQContext:Option[RabbitMQContext]
                          )
}
