package mx.cinvestav

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

  case class NodeX(nodeId:String, ip:String, port:Int, metadata:Map[String,String]= Map.empty[String,String]){
    def httpUrl = s"http://$ip:$port"
  }


  case class NodeState(
                          status:Status,
                          ip:String = "127.0.0.1",
//
                          nodesLevel0:Map[String,NodeX]  = Map.empty[String,NodeX],
                          nodesLevel0Schema:Map[String,NonEmptyList[String]] = Map.empty[String,NonEmptyList[String]],
                          nodesLevel1:Map[String,NodeX]  = Map.empty[String,NodeX],
                          nodesLevel1Schema:Map[String,NonEmptyList[String]] = Map.empty[String,NonEmptyList[String]],
  //                        NodeX -> List[Files].length
                          lb:Option[Balancer[NodeX]]=None,
//                        File -> List[NodeX]
//                          schema:Map[String,NonEmptyList[String]] = Map.empty[String,NonEmptyList[String]]
                        )
  case class NodeContext(
                            config: DefaultConfig,
                            logger: Logger[IO],
                            state:Ref[IO,NodeState],
                            rabbitMQContext: RabbitMQContext
                          )
}
