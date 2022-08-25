package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.typelevel.ci.CIString
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Declarations.{ClientX, NodeContext}

object Handshake {

  def apply()(implicit ctx:NodeContext) =  HttpRoutes.of[IO]{
    case req@POST -> Root / "handshake" => for {
      arrivalTime <- IO.monotonic.map(_.toNanos)
      response <- NoContent()
      headers  = req.headers
      clientId = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
      port     = headers.get(CIString("Port")).flatMap(_.head.value.toIntOption).getOrElse(9000)
      clientx  = ClientX(id = clientId, port = port)
      _        <- ctx.state.update{s=>
        s.copy(clients = s.clients :+ clientx )
      }
      serviceTime <- IO.monotonic.map(_.toNanos - arrivalTime)
      _ <- ctx.logger.debug(s"HANDSHAKE $clientId $serviceTime")
    } yield response
  }

}
