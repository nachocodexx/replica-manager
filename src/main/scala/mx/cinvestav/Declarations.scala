package mx.cinvestav

import cats.Order
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.commons.balancer.v3.{Balancer => BalancerV3}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, Evicted, Get, Missed, Put, RemovedNode, Replicated, SetDownloads, Uploaded}
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.typelevel.log4cats.Logger
//
import java.io.ByteArrayOutputStream
import java.util.UUID

object Declarations {

  case class User(id:UUID,bucketName:String)

  implicit val nodeXOrder: Order[NodeX] = new Order[NodeX] {
    override def compare(x: NodeX, y: NodeX): Int = Order.compare[Int](x.port,y.port)
  }
//  case class NodeX(nodeId:String, ip:String, port:Int, metadata:Map[String,String]= Map.empty[String,String]){
//    def httpUrl = s"http://$ip:$port"
//  }


//  case class SchemaMap(node:String,downloadCounter:Int)
  case class ObjectNodeKey(objectId:String,nodeId:String)
  case class ObjectId(value:String){
    def toObjectNodeKey(nodeId:String): ObjectNodeKey = ObjectNodeKey(value,nodeId)
  }


  implicit val eventXEncoder: Encoder[EventX] = {
    case x: Put => x.asJson
    case x: Get => x.asJson
    case x: Del => x.asJson
    case x: Uploaded => x.asJson
    case x: Downloaded => x.asJson
    case x: AddedNode => x.asJson
    case x: RemovedNode => x.asJson
    case x:Evicted => x.asJson
    case x: Replicated => x.asJson
    case x: Missed => x.asJson
    case x:SetDownloads => x.asJson
  }

  case class NodeState(
                        status:Status,
//                        eventCounter:
                        ip:String,
//                      Available resources
//                        AR:Map[String,NodeX]  = Map.empty[String,NodeX],
//                      OBJECT_ID -> NonEmptyList[NodeX]
//                        schema:Map[ObjectId,NonEmptyList[String]] = Map.empty[ObjectId,NonEmptyList[String]],
//                      (ObjectId,NodeId) -> DownloadCounter
//                        downloadCounter:Map[ObjectNodeKey,Int]= Map.empty[ObjectNodeKey,Int],
//
                        lb:Option[Balancer[NodeX]]=None,
                        uploadBalancer:Option[BalancerV3]=None,
                        downloadBalancer:Option[BalancerV3]=None,
                        downloadLB:Option[Balancer[NodeX]]=None,
                        events:List[EventX] = Nil
                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            state:Ref[IO,NodeState],
//                            rabbitMQContext:Option[RabbitMQContext]
                          )
}
