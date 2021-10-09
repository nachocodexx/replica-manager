package mx.cinvestav
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.{ExchangeName, QueueName, RoutingKey}
import mx.cinvestav.Declarations.{NodeContext, NodeState}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.handlers.Handlers
import mx.cinvestav.server.HttpServer
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2.{Exchange, MessageQueue, RabbitMQContext}
import mx.cinvestav.commons.balancer.v2.LoadBalancer
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig._
import pureconfig.generic.auto._

import java.net.InetAddress

object Main extends IOApp {
  implicit val config: DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  def initContext()(implicit rabbitMQContext: RabbitMQContext) = for {
    _          <- Logger[IO].debug(s"LOAD_BALANCING[${config.nodeId}]")
    _initState = NodeState(
      status   = commons.status.Up,
      ip        = InetAddress.getLocalHost.getHostAddress,
    )
    state     <- IO.ref(_initState)
    ctx        = NodeContext(config=config,logger=unsafeLogger,state=state,rabbitMQContext = rabbitMQContext)
  } yield ctx

  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig){ implicit client=>
      client.createConnection.use { implicit connection =>for {
      implicit0(rabbitMQContext:RabbitMQContext) <- IO.pure(RabbitMQContext(client = client,connection=connection))
        exchangeName   = ExchangeName(config.poolId)
          _            <- Exchange.topic(exchangeName = exchangeName)
          queueName    = QueueName(s"${config.nodeId}")
          routingKey   = RoutingKey(s"${config.poolId}.${config.nodeId}")
          _            <- MessageQueue.createThenBind(
          queueName    = queueName,
          exchangeName = exchangeName,
          routingKey   = routingKey
        )
        ctx  <- initContext()
        _ <- Handlers(queueName = queueName )(ctx=ctx).start
        _ <- HttpServer.run()(ctx=ctx )
        } yield ()
      }
    }.as(ExitCode.Success)
  }
}
