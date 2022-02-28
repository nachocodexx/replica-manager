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
      _                <- ctx.logger.debug("UPDATE_CONFIG")
      currentState     <- ctx.state.get
      rawEvents        = currentState.events
      events           = Events.orderAndFilterEventsMonotonic(events=rawEvents)
      nodes            = Events.getAllNodeXs(events=events)
      nodesLen         = nodes.length
      headers          = req.headers
      uploadBalancer   = headers.get(CIString("Upload-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.uploadLoadBalancer)
      downLoadBalancer = headers.get(CIString("Download-Load-Balancer")).map(_.head.value).getOrElse(ctx.config.downloadLoadBalancer)
//      maxAR            = headers.get(CIString("Max-Available-Resources")).flatMap(_.head.value.toIntOption).getOrElse(ctx.config.maxAr)
//      maxRF            = headers.get(CIString("Max-Replication-Factor")).flatMap(_.head.value.toIntOption).getOrElse(3)
//      balanceTemp      = headers.get(CIString("Balance-Temperature")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.balanceTemperature)
//      serviceRepDaemon = headers.get(CIString("Service-Replication-Daemon")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.serviceReplicationDaemon)
//      repStrategy               = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse(ctx.config.dataReplicationStrategy)
//      repDaemon                 = headers.get(CIString("Replication-Daemon")).flatMap(_.head.value.toBooleanOption).getOrElse(ctx.config.replicationDaemon)
//      repDaemonMillis           = headers.get(CIString("Replication-Daemon-Delay-Millis")).flatMap(_.head.value.toLongOption).getOrElse(ctx.config.replicationDaemonDelayMillis)
//      serviceRepDaemonThreshold = headers.get(CIString("Service-Replication-Threshold")).flatMap(_.head.value.toDoubleOption).getOrElse(ctx.config.serviceReplicationThreshold)
//      UPDATE UPLOAD LB
      _                <- ctx.state.update(x => x.copy(
        downloadBalancerToken = downLoadBalancer,
//        maxAR = maxAR,
//        maxRF = maxRF,
//        serviceReplicationDaemon = serviceRepDaemon,
//        serviceReplicationThreshold = serviceRepDaemonThreshold,
//        balanceTemperature = balanceTemp,
//        replicationDaemon = repDaemon,
//        replicationDaemonDelayMillis = repDaemonMillis,
//        replicationStrategy = repStrategy,
//        experimentId = ctx.config.experimentId
      ))
//      _                <- if(repStrategy == "none") currentState.replicationDaemonSingal.set(true)
//      else for {
//        _ <- currentState.replicationDaemonSingal.set(true)
//        s <- Semaphore[IO](1)
//        _ <- currentState.replicationDaemonSingal.set(false)
//        _ <- Helpers.replicationDaemon(s,period = repDaemonMillis.milliseconds,signal = currentState.replicationDaemonSingal).start.void
//      } yield ()
      _                <- Helpers.initLoadBalancerV3(uploadBalancer)
      //      UPDATE DOWNLOAD LB
      response    <- NoContent()
    } yield  response
  }

}
