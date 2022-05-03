package mx.cinvestav.server.controllers

import cats.effect._
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.events.Events
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
import scala.concurrent.duration._
import scala.language.postfixOps
//
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.commons.payloads.AddCacheNode
import org.typelevel.ci.CIString

object UpdateConfig {
//  case class Payload(nodeId:String, ip:String, port:Int, metadata:Map[String,String])

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root / "update" =>  for {
      _                    <- ctx.logger.debug("UPDATE_CONFIG")
      currentState         <- ctx.state.get
      rawEvents            = currentState.events
      events               = Events.orderAndFilterEventsMonotonic(events=rawEvents)
      nodes                = Events.getAllNodeXs(events=events)
      nodesLen             = nodes.length
      headers              = req.headers
      uploadBalancer       = headers.get(CIString("Upload-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.uploadLoadBalancer)
      downLoadBalancer     = headers.get(CIString("Download-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.downloadLoadBalancer)
      availableResources   = headers.get(CIString("Available-Resources")).flatMap(_.head.value.toIntOption).getOrElse(ctx.config.availableResources)
      replicationFactor    = headers.get(CIString("Replication-Factor")).flatMap(_.head.value.toIntOption).getOrElse(ctx.config.replicationFactor)
      replicationTechnique = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
      _                <- ctx.state.update(x => x.copy(
        downloadBalancerToken = downLoadBalancer,
        uploadBalancerToken  = uploadBalancer,
        replicationFactor    = replicationFactor,
        availableResources   = availableResources,
        replicationTechnique = replicationTechnique
      ))
      response    <- NoContent()
    } yield  response
  }

}
