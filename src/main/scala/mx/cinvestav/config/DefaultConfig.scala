package mx.cinvestav.config

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

  def url:String = s"$protocol://${ip}:$port"
  def apiUrl:String = s"${this.url}/api/$apiVersion"
//  def createCacheNode
}

case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          host:String,
                          port:Int,
                          maxRf:Int,
                          systemReplication:SystemReplication,
                          rabbitmq: RabbitMQClusterConfig
                        )
