package mx.cinvestav

import cats.Order
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import io.chrisdavenport.mules.MemoryCache
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.v2.RabbitMQContext
import org.typelevel.log4cats.Logger

import java.io.ByteArrayOutputStream
import java.util.UUID

object Declarations {

  case class User(id:UUID,bucketName:String)

  implicit val nodeXOrder = new Order[NodeX] {
    override def compare(x: NodeX, y: NodeX): Int = Order.compare[Int](x.port,y.port)
  }
  case class NodeX(nodeId:String, ip:String, port:Int, metadata:Map[String,String]= Map.empty[String,String]){
    def httpUrl = s"http://$ip:$port"
  }


//  case class SchemaMap(node:String,downloadCounter:Int)
  case class ObjectNodeKey(objectId:String,nodeId:String)
  case class ObjectId(value:String)
  case class NodeState(
                        status:Status,
                        ip:String,
//                      Available resources
                        AR:Map[String,NodeX]  = Map.empty[String,NodeX],
//                      OBJECT_ID -> NonEmptyList[NodeX]
                        schema:Map[ObjectId,NonEmptyList[String]] = Map.empty[ObjectId,NonEmptyList[String]],
//                      (ObjectId,NodeId) -> DownloadCounter
                        downloadCounter:Map[ObjectNodeKey,Int]= Map.empty[ObjectNodeKey,Int],
//
                        lb:Option[Balancer[NodeX]]=None,
                        downloadLB:Option[Balancer[NodeX]]=None,
                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            state:Ref[IO,NodeState],
                            rabbitMQContext: RabbitMQContext
                          )
}
