package mx.cinvestav.config

import cats.implicits._
import cats.effect._
import io.circe.Json
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.docker.Image
import mx.cinvestav.commons.types.SystemReplicationResponse
import org.http4s._
//import org.http4s.Me
//import org.http4s.blaze.http.http2.PseudoHeaders.Method
import org.http4s.{Request, Uri}
//
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._

trait NodeInfo {
    def protocol: String
    def ip: String
    def hostname: String
    def port: Int
    def apiVersion: String
}

case class DataReplicationSystem(hostname:String, port:Int, apiVersion:Int){
  def url:String = s"http://$hostname:$port"
  def apiUrl:String = s"${this.url}/api/v$apiVersion"
  def reset()(implicit ctx:NodeContext) = for {
    _   <- IO.unit
    req =  Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"$apiUrl/reset")
    )
    s   <- ctx.client.status(req)
    _   <- ctx.logger.info(s"RESET_STATUS $s")
  } yield ()
}
case class SystemReplication(protocol:String="http", ip:String="127.0.0.1", hostname:String="localhost",
                              port:Int=1025,
                              apiVersion:String="v2"
                            ) extends NodeInfo{

  def url:String = s"$protocol://$hostname:$port"
  def apiUrl:String = s"${this.url}/api/$apiVersion"
  def createNodeStr:String = s"${this.apiUrl}/create/cache-node"
  def createNodeUri:Uri = Uri.unsafeFromString(s"http://${hostname}:${port}/api/v2/nodes")
//
//  def createNode() = {

//  }

  def createNode(
                  cacheSize:Int = 100,
                  policy:String = "LFU",
                  networkName:String = "my-net",
                  environments:Map[String,String]= Map.empty[String,String],
                  image:Image =Image(repository = "nachocode/cache-node", tag = Some("v2"))
                )(implicit ctx:NodeContext) = {
    val json = Json.obj(
      "cacheSize" -> cacheSize.asJson,
      "policy" -> policy.asJson,
      "networkName" -> networkName.asJson,
      "environments" -> environments.asJson,
      "image" -> image.asJson
    )
    val req = Request[IO](
      method = Method.POST,
      uri    = this.createNodeUri
//      uri    = Uri.unsafeFromString(s"http://$hostname:$port/api/v$apiVersion/nodes")
    ).withEntity(json)
    //      .withEntity(json)
    for {
      _      <- ctx.logger.debug(s"CREATE_NODE_URI ${this.createNodeUri}")
      status <- ctx.client.expect[SystemReplicationResponse](req)
      _      <- ctx.logger.debug(s"CREATE_NODE_STATUS $status")
    } yield status

  }
  def launchNode()(implicit ctx:NodeContext) = for {
    _                       <- IO.unit
    systemReplicationSignal = ctx.systemReplicationSignal
    currentSignalValue      <- systemReplicationSignal.get
    _                       <- ctx.logger.debug(s"UPLOAD_SIGNAL_VALUE $currentSignalValue")
    res                       <- if(ctx.config.systemReplicationEnabled && !currentSignalValue)
    ctx.systemReplicationSignal.set(true) *> createNode().map(_.some)  <* systemReplicationSignal.set(false)
    else IO.pure(None)
  }  yield res

  def reset()(implicit ctx:NodeContext) = for {
    _   <- IO.unit
    req =  Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"$apiUrl/reset")
    )
    s   <- ctx.client.status(req)
    _   <- ctx.logger.info(s"RESET_STATUS $s")
  } yield ()
}

case class DefaultConfig(
                          port:Int,
                          nodeId:String,
                          host:String,
                          systemReplication:SystemReplication,
                          uploadLoadBalancer:String,
                          downloadLoadBalancer:String,
                          returnHostname:Boolean,
                          cloudEnabled:Boolean,
                          hasNextPool:Boolean,
                          apiVersion:Int,
                          dataReplication:DataReplicationSystem,
                          daemonDelayMs:Int,
                          usePublicPort:Boolean,
                          maxConnections:Int,
                          bufferSize:Int,
                          responseHeaderTimeoutMs:Long,
                          nSemaphore:Int = 1,
                          replicationTechnique:String,
                          systemReplicationEnabled:Boolean,
                          defaultImpactFactor:Double = 0.0,
                          availableResources:Int,
                          replicationFactor:Int,
                          elasticity:Boolean
                          //                          rabbitmq: RabbitMQClusterConfig
                        )
