package mx.cinvestav.server.middlewares

import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import mx.cinvestav.Declarations.{NodeContext, User}
import org.http4s.Request
import org.http4s.server.AuthMiddleware
import org.typelevel.ci.CIStringSyntax

import java.util.UUID

object AuthMiddlewareX {

  def authUser()(implicit ctx:NodeContext):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(IO.unit)
      headers    = req.headers
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      //      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = userId,bucketName=bucketName  ).pure[IO])
        } yield x
        case (Some(_),None) => OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.none[IO,User]
        case (None,None )   => OptionT.none[IO,User]
      }

    } yield ress
    }

  def apply(implicit ctx:NodeContext): AuthMiddleware[IO, User] =
    AuthMiddleware(authUser=authUser)

}
