package mx.cinvestav.server.controllers

import cats.effect.IO
import cats.implicits._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.EventX
import mx.cinvestav.events.Events
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import scala.concurrent.ExecutionContext.global
import mx.cinvestav.Declarations.Implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._


object SaveEventsController {

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case GET -> Root / "events/all" => for {

      currentState       <- ctx.state.get
      rawEvents          = currentState.events
      events             = Events.orderAndFilterEventsMonotonic(events=rawEvents)
      ars                = Events.getAllNodeXs(events = events)
      nodeURLs            = ars.map(x=>(x.nodeId,x.httpUrl)).map(x=>(x._1,s"${x._2}/api/v6/events"))
      (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
      requests           = nodeURLs.map(x=>
        (x._1,Request[IO](method = GET,uri = Uri.unsafeFromString(x._2)))
      )
      responses          <- requests.traverse{
        request =>
          client.expect[List[EventX]](request._2).map{ events=>
            (request._1,events)
          }
      }.map(_.toMap.asJson)
        .onError{ e =>
          ctx.logger.error(e.getMessage)
        }
      x = Map(ctx.config.nodeId -> rawEvents ).asJson
//      _ <- ctx.logger.debug(x.toString)
      _                  <- finalizer
      response <- Ok(responses.deepMerge(x))
    } yield response
  }

}
