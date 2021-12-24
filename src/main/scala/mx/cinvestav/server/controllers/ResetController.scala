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
    case POST@req -> Root =>
      for {
        _   <- ctx.logger.debug("RESET")
        lb           <- Helpers.initLoadBalancerV3(balancerToken = ctx.config.uploadLoadBalancer)
        currentState <- ctx.state.get
        nodes        = Events.getAllNodeXs(events=currentState.events)
        _            <- ctx.state.update{ s=>
          s.copy(
            events =  currentState.events.filter{
              case _:events.AddedNode => true
              case _=>false
            },
            uploadBalancer = lb.some,
            monitoringEvents = Nil,
            monitoringEx = Map.empty[String,EventX]
          )

        }

        x <- nodes.map{node=>
          val uri = if(ctx.config.returnHostname) s"http://${node.nodeId}:${node.port}/api/v6/reset" else s"http://${node.ip}:${node.port}/api/v6/reset"
          Request[IO](
            method = Method.POST,
            uri    = Uri.unsafeFromString(uri)
          )
        }.traverse{req=>ctx.client.status(req)}
        _   <- ctx.logger.debug(x.toString)
        res <- NoContent()
      } yield res
  }

}
