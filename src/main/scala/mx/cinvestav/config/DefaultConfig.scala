package mx.cinvestav.config

import org.http4s.Uri

trait NodeInfo {
    def protocol: String
    def ip: String
    def hostname: String
    def port: Int
    def apiVersion: String
  }
case class SystemReplication(
                              protocol:String="http",
                              ip:String="127.0.0.1",
                              hostname:String="localhost",
                              port:Int=3000,
                              apiVersion:String="v6"
                            ) extends NodeInfo{

  def url:String = s"$protocol://$hostname:$port"
  def apiUrl:String = s"${this.url}/api/$apiVersion"
  def createNodeStr:String = s"${this.apiUrl}/create/cache-node"
  def createNodeUri:Uri = Uri.unsafeFromString(
    s"${this.apiUrl}/create/cache-node"
  )

//  def createCacheNode
}

case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          host:String,
                          port:Int,
                          maxRf:Int,
                          maxAr:Int,
                          systemReplication:SystemReplication,
                          serviceReplicationDaemon:Boolean,
                          serviceReplicationThreshold:Double,
                          serviceReplicationDaemonDelay:Long,
                          replicationDaemon:Boolean,
                          replicationDaemonDelayMillis:Long,
                          balanceTemperature:Boolean,
                          uploadLoadBalancer:String="UF",
                          downloadLoadBalancer:String="LEAST_DOWNLOADS",
                          defaultCacheSize:Int,
                          defaultCachePolicy:String,
                          defaultCachePort:Int,
                          hostLogPath:String,
                          downloadMaxRetry:Int,
                          downloadBaseDelayMs:Long
                          //                          rabbitmq: RabbitMQClusterConfig
                        )
