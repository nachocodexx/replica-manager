package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events
import mx.cinvestav.commons.events.EventX
import mx.cinvestav.events.Events
import org.http4s.{HttpRoutes, Request, Uri,Method}
import org.http4s.implicits._
import org.http4s.dsl.io._

object ResetController {


  def apply()(implicit ctx:NodeContext):HttpRoutes[IO]= HttpRoutes.of[IO]{
    case POST@req -> Root / "reset" =>
      for {
        _            <- ctx.logger.debug("RESET")
        currentState <- ctx.state.get
        nodes        = Events.getAllNodeXs(events=currentState.events)
        _            <- ctx.state.update{ s=>
          s.copy(
            events =  currentState.events.filter{
              case _:events.AddedNode => true
              case _:events.RemovedNode => true
              case _:Events.UpdatedNetworkCfg => true
              case _=>false
            },
          )

        }
        apiVersion = s"v${ctx.config.apiVersion}"
        x   <- nodes.map{node=>

          val uri = if(ctx.config.returnHostname) s"http://${node.nodeId}:${node.port}/api/$apiVersion/reset"
            else s"http://${node.ip}:${node.port}/api/$apiVersion/reset"

          Request[IO](
            method = Method.POST,
            uri    = Uri.unsafeFromString(uri)
          )

        }.traverse{req=>ctx.client.status(req)}
        _   <- ctx.config.systemReplication.reset().start
        _   <- ctx.config.dataReplication.reset().start
        _   <- ctx.logger.debug(x.toString)
        res <- NoContent()
      } yield res
  }

}
