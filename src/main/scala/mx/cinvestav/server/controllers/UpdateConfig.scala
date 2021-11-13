package mx.cinvestav.server.controllers

import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events

import java.util.UUID
//
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
//
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.commons.payloads.AddCacheNode
import org.typelevel.ci.CIString

object UpdateConfig {
//  case class Payload(nodeId:String, ip:String, port:Int, metadata:Map[String,String])

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root  => for {
      currentState     <- ctx.state.get
      rawEvents        = currentState.events
      events           = Events.orderAndFilterEventsMonotonic(events=rawEvents)
      nodes            = Events.getAllNodeXs(events=events)
      nodesLen         = nodes.length
      headers          = req.headers
      uploadBalancer   = headers.get(CIString("Upload-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.uploadLoadBalancer)
      downLoadBalancer = headers.get(CIString("Download-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.downloadLoadBalancer)
      maxAR            = headers.get(CIString("Max-Available-Resources")).flatMap(_.head.value.toIntOption).getOrElse(ctx.config.maxAr)
      maxRF            = headers.get(CIString("Max-Replication-Factor")).flatMap(_.head.value.toIntOption).getOrElse(ctx.config.maxRf)
      balanceTemp      = headers.get(CIString("Balance-Temperature")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.balanceTemperature)
      serviceRepDaemon = headers.get(CIString("Service-Replication-Daemon")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.serviceReplicationDaemon)
      repDaemon                 = headers.get(CIString("Replication-Daemon")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.replicationDaemon)
      repDaemonMillis           = headers.get(CIString("Replication-Daemon-Delay-Millis")).flatMap(_.head.value.toLongOption).getOrElse(ctx.config.replicationDaemonDelayMillis)
      serviceRepDaemonThreshold = headers.get(CIString("Service-Replication-Threshold")).flatMap(_.head.value.toDoubleOption).getOrElse(ctx.config.serviceReplicationThreshold)
//      UPDATE UPLOAD LB
      _                <- Helpers.initLoadBalancerV3(uploadBalancer)
      //      UPDATE DOWNLOAD LB
      _                <- ctx.state.update(x => x.copy(
        downloadBalancerToken = downLoadBalancer,
        maxAR = maxAR,
        maxRF = maxRF,
        serviceReplicationDaemon = serviceRepDaemon,
        serviceReplicationThreshold = serviceRepDaemonThreshold,
        balanceTemperature = balanceTemp,
        replicationDaemon = repDaemon,
        replicationDaemonDelayMillis = repDaemonMillis
      ))
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)

      response    <- Ok("UPDATED_BALANCER")
    } yield  response
  }

}
