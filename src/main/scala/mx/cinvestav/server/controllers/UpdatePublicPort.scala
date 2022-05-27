package mx.cinvestav.server.controllers

import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.commons.events.ServiceReplicator.AddedStorageNode
import mx.cinvestav.events.Events
import mx.cinvestav.events.Events.UpdatedNetworkCfg

import java.util.UUID
//
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.io._
//
import io.circe.generic.auto._
//
import org.typelevel.ci.CIString

object UpdatePublicPort {

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root / "nodes" / nodeId / "network-cfg"  => for {
      arrivalTime      <- IO.realTime.map(_.toMillis)
      currentState     <- ctx.state.get
      rawEvents        = currentState.events
      headers          = req.headers
      maybePublicPort  = headers.get(CIString("Public-Port")).map(_.head.value).flatMap(_.toIntOption)
      ipAddress        = headers.get(CIString("Ip-Address")).map(_.head.value).getOrElse("127.0.0.1")
      _ <- ctx.logger.debug(s"UPDATE_NETWORK $nodeId")
      response         <- maybePublicPort match {
        case Some(publicPort) => for  {
          _         <- IO.unit
          newEvent     = UpdatedNetworkCfg(
            nodeId     = nodeId,
            poolId     = ctx.config.nodeId,
            timestamp  = arrivalTime,
            publicPort = publicPort,
            ipAddress  = ipAddress
          )
          _         <- ctx.state.update{ s=>
            s.copy(
              nodes = s.nodes.updatedWith(nodeId)(o => o.map(oo=>oo.copy(metadata = oo.metadata + ("PUBLIC_PORT"->publicPort.toString)  )))
            )
          }
          _         <- Events.saveEvents(events = newEvent::Nil)
          res       <- NoContent()
        } yield res
        case None => Forbidden()
      }
//      response         <- NoContent()
    } yield  response
  }

}
