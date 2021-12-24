package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.events.Events
import mx.cinvestav.events.Events.MonitoringStats
import org.http4s.dsl.io._
import org.http4s.{Headers, HttpRoutes}
import org.typelevel.ci.CIString

object MonitoringController {

  def apply()(implicit ctx:NodeContext)= HttpRoutes.of[IO]{
    case req@POST -> Root / "monitoring" / nodeId   => for {
      arrivalTime             <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos        <- IO.monotonic.map(_.toNanos)
      headers       = req.headers
      newEvent = Helpers.getMonitoringStatsFromHeaders(nodeId,arrivalTime)(headers)
      _ <- Events.saveMonitoringEvents(event= newEvent)
//      _ <-
//      _             <- ctx.logger.debug(s"TOTAL_RAM $nodeId $totalRAM")
//      _             <- ctx.logger.debug(s"FREE_RAM $nodeId $freeRAM")
//      _             <- ctx.logger.debug(s"USED_RAM $nodeId $usedRAM")
//      _             <- ctx.logger.debug(s"UF_RAM $nodeId $ufRAM")
//      _             <- ctx.logger.debug(s"SYSTEM_CPU_LOAD $nodeId $systemCpuLoad")
//      _             <- ctx.logger.debug(s"CPU_LOAD $nodeId $cpuLoad")
//      _             <- ctx.logger.debug("____________________________________________")
      res           <- NoContent()
    } yield res
  }

}
