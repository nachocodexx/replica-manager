package mx.cinvestav.config

case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          host:String,
                          port:Int,
                          rabbitmq: RabbitMQClusterConfig
                        )
