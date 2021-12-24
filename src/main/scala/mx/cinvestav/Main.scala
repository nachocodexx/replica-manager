package mx.cinvestav
import cats.effect.std.{Queue, Semaphore}
import fs2.concurrent.SignallingRef
import cats.implicits._
import cats.effect._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
//
//import mx.cinvestav.commons.eve
import mx.cinvestav.Declarations.{NodeContext, NodeState}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.server.HttpServer
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
  val threadPool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
//  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  def initContext(client:Client[IO]): IO[NodeContext] = for {
    _          <- Logger[IO].debug(s"CACHE_POOL[${config.nodeId}]")
    //    s          <- Semaphore[IO](1)
    signalRef <- SignallingRef[IO,Boolean](false)
    systemSemaphore <- Semaphore[IO](1)
    _initState = NodeState(
      status   = commons.status.Up,
      ip       = InetAddress.getLocalHost.getHostAddress,
//      s        = s,
      downloadBalancerToken = config.downloadLoadBalancer,
//      systemRepSignal = systemRepSignal,
      serviceReplicationThreshold = config.serviceReplicationThreshold,
      maxAR = config.maxAr,
      maxRF = config.maxRf,
      serviceReplicationDaemon = config.serviceReplicationDaemon,
      balanceTemperature = config.balanceTemperature,
      replicationDaemon = config.replicationDaemon,
      replicationDaemonDelayMillis = config.replicationDaemonDelayMillis,
      systemSemaphore = systemSemaphore,
      experimentId = config.experimentId,
      replicationStrategy = config.dataReplicationStrategy,
      replicationDaemonSingal = signalRef
    )
    state      <- IO.ref(_initState)
    ctx        = NodeContext(config=config,logger=unsafeLogger,state=state,errorLogger = unsafeErrorLogger,client=client)
  } yield ctx

  override def run(args: List[String]): IO[ExitCode] = {
    for {
//      sUpload      <- Semaphore[IO](1)
      (client,finalizer) <- BlazeClientBuilder[IO](global)
        .withDefaultSocketReuseAddress
        .resource.allocated
      sDownload          <- Semaphore[IO](1)
//      sReplication       <- Semaphore[IO](1)
//      signalRef          <- SignallingRef.of[IO,Boolean](false)
      implicit0(ctx:NodeContext) <- initContext(client)
//      _                  <- Helpers.replicationDaemon(sReplication,period = config.replicationDaemonDelayMillis milliseconds,signalRef)(ctx=ctx).startOn(threadPool).void
//      _                  <- Helpers.serviceReplicationDaemon(s=signalRef,period = config.serviceReplicationDaemonDelay milliseconds)(ctx=ctx).start.void
      _                  <- HttpServer(sDownload).run()
      _                  <- finalizer
    } yield (ExitCode.Success)
  }
}
// Agregar descriptiva estado del arte
// Marco teorio
