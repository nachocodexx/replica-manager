package mx.cinvestav.server

import cats.data.{Kleisli, NonEmptyList, OptionT}
import cats.implicits._
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.commons.events.{Downloaded, Evicted, Missed, Uploaded}
import mx.cinvestav.events.Events
import mx.cinvestav.server.controllers.{AddNode, DownloadController, DownloadControllerV2, EventsControllers, EvictedController, MonitoringController, PutController, ResetController, SaveEventsController, StatsController, UpdateConfig, UploadContraoller, UploadControllerV2}
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
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//

object HttpServer {
//  ____________________________________________
  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           serviceTimeNanos:Long,
                           timestamp:Long,
                           level:Int
                         )
  case class ReplicationResponse(guid:String,replicas:List[String],milliSeconds:Long,timestamp:Long,rf:Int=1)
//  ________________________

//  def authMiddleware(implicit ctx:NodeContext):AuthMiddleware[IO,User] =
//    AuthMiddleware(authUser=authUser)

//  def authRoutes()(implicit ctx:NodeContext):AuthedRoutes[User,IO] =
//    UploadContraoller() <+> DownloadController()
//  <+> UploadControllerV2()
//  <+> GetController()

  private def httpApp(sDownload:Semaphore[IO])(implicit ctx:NodeContext): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
//      "/api/v6" ->  AuthMiddlewareX(ctx=ctx)(authRoutes()),
      "/api/v6/update" -> UpdateConfig(),
      "/api/v6/add-node" -> AddNode(),
      "/api/v6/stats" -> CORS(StatsController()),
      "/api/v6/events" -> CORS(EventsControllers()),
      "/api/v6/events/all" -> CORS(SaveEventsController()),
      "/api/v7/reset" -> ResetController(),
      "/api/v7" -> AuthMiddlewareX(ctx)(DownloadControllerV2(sDownload) <+> UploadControllerV2(sDownload)),
      "/api/v7/evicted" -> EvictedController(),
      "/api/v7/put" -> PutController(),
      "/api/v7/monitoring" -> MonitoringController(),
//      "/api/v7"
    ).orNotFound

  def run(sDownload:Semaphore[IO])(implicit ctx:NodeContext): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp(sDownload))
      .serve
      .compile
      .drain
  } yield ()


}
