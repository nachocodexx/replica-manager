package mx.cinvestav.server

import cats.data.{Kleisli, NonEmptyList, OptionT}
import cats.implicits._
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.commons.events.{Downloaded, Evicted, Missed, Uploaded}
import mx.cinvestav.events.Events
import mx.cinvestav.server.controllers.{AddNode, CompletedUploadController, DownloadControllerV2, EventsControllers, EvictedController, PutController, ReplicateController, ResetController, SaveEventsController, StatsController, UpdateConfig, UpdatePublicPort, UploadControllerV2, UploadV3}
import mx.cinvestav.server.middlewares.AuthMiddlewareX
import org.http4s.server.middleware.CORS
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.Helpers
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
//
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Declarations.Implicits._
//
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.server.AuthMiddleware
import org.typelevel.ci._
//
import scala.concurrent.duration._
import language.postfixOps
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//

class HttpServer(s:Semaphore[IO])(implicit ctx:NodeContext) {
  def apiBaseRouteName = s"/api/v${ctx.config.apiVersion}"

  def basicOpsRoutes = AuthMiddlewareX(ctx)(DownloadControllerV2(s) <+> UploadControllerV2(s))

  def defaultRoutes =
   CompletedUploadController() <+>
     UpdateConfig() <+>
     AddNode() <+>
     CORS(StatsController()) <+>
     CORS(EventsControllers()) <+>
     CORS(SaveEventsController()) <+> ResetController() <+> PutController()<+> EvictedController() <+> UpdatePublicPort() <+> ReplicateController()


  def httpApp: Kleisli[IO, Request[IO], Response[IO]] =
    Router[IO](
      s"$apiBaseRouteName"-> basicOpsRoutes,
      s"$apiBaseRouteName" -> defaultRoutes,
      s"/api/v3/" -> UploadV3(s = s)
//      "/api/v6/update" -> UpdateConfig(),
//      "/api/v6/add-node" -> AddNode(),
//      "/api/v6/stats" -> CORS(StatsController()),
//      "/api/v6/events" -> CORS(EventsControllers()),
//      "/api/v6/events/all" -> CORS(SaveEventsController()),
//      "/api/v7/reset" -> ResetController(),
//      "/api/v7" ->
//      "/api/v7/evicted" -> EvictedController(),
//      "/api/v7/put" -> PutController(),
//      "/api/v7/monitoring" -> MonitoringController(),
      //      "/api/v7"
    ).orNotFound


  def run(): IO[Unit] = BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp)
      .withMaxConnections(ctx.config.maxConnections)
      .withBufferSize(ctx.config.bufferSize)
      .withResponseHeaderTimeout(ctx.config.responseHeaderTimeoutMs milliseconds)
      .serve
      .compile
      .drain

}

object HttpServer {
//  ____________________________________________

  def apply(sDownload:Semaphore[IO])(implicit ctx:NodeContext) = new HttpServer(sDownload)
//    for {
//    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
//    _ <-
//  } yield ()


}
