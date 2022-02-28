package mx.cinvestav.config

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import org.http4s._
//import org.http4s.Me
//import org.http4s.blaze.http.http2.PseudoHeaders.Method
import org.http4s.{Request, Uri}

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
                              port:Int=3000,
                              apiVersion:String="v2"
                            ) extends NodeInfo{

  def url:String = s"$protocol://$hostname:$port"
  def apiUrl:String = s"${this.url}/api/$apiVersion"
  def createNodeStr:String = s"${this.apiUrl}/create/cache-node"
  def createNodeUri:Uri = Uri.unsafeFromString(s"http://${hostname}:${port}/api/v2/create/cache-node")
//
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
//                          maxRf:Int,
//                          maxAr:Int,
                          nodeId:String,
                          host:String,
                          systemReplication:SystemReplication,
//                          serviceReplicationDaemon:Boolean,
//                          serviceReplicationThreshold:Double,
//                          serviceReplicationDaemonDelay:Long,
//                          replicationDaemon:Boolean,
//                          replicationDaemonDelayMillis:Long,
//                          balanceTemperature:Boolean,
                          uploadLoadBalancer:String,
//                          ="UF",
                          downloadLoadBalancer:String,
//                          "LEAST_DOWNLOADS",
//                          defaultCacheSize:Int,
//                          defaultCachePolicy:String,
//                          defaultCachePort:Int,
//                          hostLogPath:String,
//                          dataReplicationStrategy:String="static",
//                          dataReplicationIntervalMs:Long=10000,
                          returnHostname:Boolean,
                          cloudEnabled:Boolean,
                          hasNextPool:Boolean,
                          apiVersion:Int,
                          dataReplication:DataReplicationSystem,
                          monitoringDelayMs:Int,
                          usePublicPort:Boolean,
                          maxConnections:Int,
                          bufferSize:Int,
                          responseHeaderTimeoutMs:Long
                          //                          rabbitmq: RabbitMQClusterConfig
                        )
