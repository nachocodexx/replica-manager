package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{Evicted, Get, Put}
import mx.cinvestav.events.Events
import mx.cinvestav.commons.payloads.PutAndGet
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
//      events
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityDecoder._
//
object PutController {

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "put" => for {
      _ <- ctx.logger.debug("REPLICATED!!!")
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      putGet           <- req.as[PutAndGet].onError{ e=>ctx.errorLogger.error(e.getMessage)}
      putEvent         = putGet.put
      getEvent         = putGet.get
      objectId         = putEvent.objectId
      selectedNodeId   = putEvent.nodeId
      serviceTimeNanos = putEvent.serviceTimeNanos
      operationId      = putEvent.correlationId
      _                <- Events.saveEvents(events = List(putEvent,getEvent))
      _                <- ctx.logger.info(s"UPLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
      response         <- NoContent()
    } yield response
  }

}
