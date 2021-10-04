package mx.cinvestav

//
import cats.implicits._
import cats.effect._
//
import org.http4s._
//import org.http4s.implicits._
import org.http4s.blaze.client.BlazeClientBuilder
//
import concurrent.ExecutionContext.global

object Helpers {

  def redirectTo(nodeUrl:String,req:Request[IO]) = for {
    _                   <- IO.unit
    newReq             = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    response           <- client.toHttpApp.run(newReq)
    _ <- finalizer
  } yield response

}
