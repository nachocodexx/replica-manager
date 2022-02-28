package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.Evicted
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.events.Events
import org.http4s.dsl.io._
import org.http4s.HttpRoutes
//      events
import org.http4s.circe.CirceEntityDecoder._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object EvictedController {


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "evicted" => for {
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
//      currentState     <- ctx.state.get
      evictedEvent     <- req.as[Evicted].onError{ e=>
        ctx.errorLogger.error(e.getMessage)
      }
      _                <- Events.saveEvents(
        events = List(evictedEvent)
      )
      _                <- ctx.logger.debug(s"EVICTED ${evictedEvent.nodeId} ${evictedEvent.objectId}")
      response <- NoContent()
    } yield response
  }

}
