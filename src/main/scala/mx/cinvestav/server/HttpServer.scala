package mx.cinvestav.server

import cats.data.{Kleisli, OptionT}
import cats.implicits._
import cats.effect.IO
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.multipart.Multipart
import org.http4s.server.Router
//
import mx.cinvestav.Declarations.{NodeContext, User}
//
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.server.AuthMiddleware
import org.typelevel.ci._
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//
object HttpServer {

  def authUser()(implicit ctx:NodeContext):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(ctx.logger.debug("AUTH MIDDLEWARE"))
      headers    = req.headers
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = UUID.fromString(userId),bucketName=bucketName  ).pure[IO])
          //          _  <- OptionT.liftF(ctx.logger.debug("AUTHORIZED"))
        } yield x
        case (Some(_),None) => OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.none[IO,User]
        case (None,None )   => OptionT.none[IO,User]
      }

    } yield ress
    }

  def authMiddleware(implicit ctx:NodeContext):AuthMiddleware[IO,User] =
    AuthMiddleware(authUser=authUser)

  def authRoutes()(implicit ctx:NodeContext):AuthedRoutes[User,IO] = AuthedRoutes.of[User,IO] {
    case authReq@GET -> Root / "download" / UUIDVar(guid)   as user => for {
      arrivalTime  <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
      nodesLvl0    = currentState.nodesLevel0
      nodesLvl1    = currentState.nodesLevel1
      req          = authReq.req
//
      response     <- if(nodesLvl0.isEmpty && nodesLvl1.isEmpty) for {
        response <- ServiceUnavailable()
      } yield response
      else  for {
        _                  <- IO.unit
        nodeUrl            = "http://localhost:4000"
        newReq             = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
        (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
        newResponse        <- client.toHttpApp.run(newReq)
        _                  <- ctx.logger.debug(newResponse.toString())
        _                  <- finalizer
//        response <- Ok("EVERYTHING OK!")
      } yield newResponse

    } yield response
//
    case authReq@POST -> Root / "upload"   as user => for {
      arrivalTime        <- IO.realTime.map(_.toMillis)
      currentState       <- ctx.state.get
//      nodes              = currentState.nodesLevel0
      req                = authReq.req
      headers            = req.headers
      level              = headers.get(CIString("Storage-Level")).flatMap(_.head.value.toIntOption)
      response           <- level match {
        case Some(storageLevel) =>   storageLevel match {
          case 0 => for {
            response <- Ok("LEVEL 0")
          } yield response
          case 1 => for {
            response <- Ok("LEVEL 1")
          } yield response
        }
        case None => ServiceUnavailable()
      }

//      nodeUrl            = "http://localhost:4000"
//      newReq             = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
//      (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
//      newResponse        <- client.toHttpApp.run(newReq)
//      _ <- ctx.logger.debug(newResponse.toString())
//      _ <- finalizer
    } yield response

  }
  private def httpApp()(implicit ctx:NodeContext): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/api/v6" ->  authMiddleware(ctx=ctx)(authRoutes()),
    ).orNotFound

  def run()(implicit ctx:NodeContext): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp())
      .serve
      .compile
      .drain
  } yield ()
}
