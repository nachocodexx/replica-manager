package mx.cinvestav
import breeze.linalg._
import cats.data.NonEmptyList
import cats.effect.std.Semaphore
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.events.Uploaded
import org.http4s.Request
import org.http4s.blaze.client.BlazeClientBuilder
//{*, sum}
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.events.Events
//
//import mx.cinvestav.commons.eve
import mx.cinvestav.Declarations.{NodeContext, NodeState}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.server.HttpServer
import mx.cinvestav.utils.RabbitMQUtils
//
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import pureconfig._
import pureconfig.generic.auto._
//
import concurrent.duration._
import language.postfixOps
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import java.net.InetAddress

object Main extends IOApp {
  implicit val config: DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
//  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  def initContext(): IO[NodeContext] = for {
    _          <- Logger[IO].debug(s"CACHE_POOL[${config.nodeId}]")
    //    s          <- Semaphore[IO](1)
    systemRepSignal <- SignallingRef[IO,Boolean](false)
    systemSemaphore <- Semaphore[IO](1)
    _initState = NodeState(
      status   = commons.status.Up,
      ip       = InetAddress.getLocalHost.getHostAddress,
//      s        = s,
      downloadBalancerToken = config.downloadLoadBalancer,
      systemRepSignal = systemRepSignal,
      serviceReplicationThreshold = config.serviceReplicationThreshold,
      maxAR = config.maxAr,
      maxRF = config.maxRf,
      serviceReplicationDaemon = config.serviceReplicationDaemon,
      balanceTemperature = config.balanceTemperature,
      replicationDaemon = config.replicationDaemon,
      replicationDaemonDelayMillis = config.replicationDaemonDelayMillis,
      systemSemaphore = systemSemaphore
    )
    state      <- IO.ref(_initState)
    ctx        = NodeContext(config=config,logger=unsafeLogger,state=state,errorLogger = unsafeErrorLogger)
  } yield ctx

  override def run(args: List[String]): IO[ExitCode] = {
    for {
          ctx <- initContext()
          _ <- Helpers.replicationDaemon(period = config.replicationDaemonDelayMillis milliseconds)(ctx=ctx).start.void
          _ <- Helpers.serviceReplicationDaemon(period = config.serviceReplicationDaemonDelay milliseconds)(ctx=ctx).start.void
          _ <- HttpServer.run()(ctx=ctx )
    } yield (ExitCode.Success)
  }
}
// Agregar descriptiva estado del arte
// Marco teorio
