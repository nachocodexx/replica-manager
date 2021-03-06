package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import mx.cinvestav.events.Events
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.Declarations.Implicits._
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//_______________________________________
object EventsControllers {

  object OptionalFiltered extends OptionalQueryParamDecoderMatcher[Boolean]("filtered")

  def apply()(implicit ctx:NodeContext) = {
//   object
    HttpRoutes.of[IO]{
      case GET -> Root :? OptionalFiltered(filtered)  => for {
        currentState <- ctx.state.get
        rawEvents    = currentState.events
        events       = filtered match {
          case Some(isFiltered) =>
            if(isFiltered ) Events.filterEvents(events = EventXOps.OrderOps.byTimestamp(events=rawEvents).reverse)
            else EventXOps.OrderOps.byTimestamp(rawEvents).reverse
          case None => EventXOps.OrderOps.byTimestamp(rawEvents).reverse
        }

        response     <- Ok(events.asJson)
      } yield response
    }
  }

}
