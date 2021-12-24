package mx.cinvestav.server.controllers

import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{Evicted, Get, Put}
import mx.cinvestav.events.Events
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
//      events
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityDecoder._

object PutController {


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "put" => for {
      _ <- ctx.logger.debug("UPLOADED!!!")
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      //      currentState     <- ctx.state.get
      putEvent     <- req.as[Put].onError{ e=>
        ctx.errorLogger.error(e.getMessage)
      }
      getEvent = Get(
        serialNumber = 0,
        nodeId = putEvent.nodeId,
        objectId = putEvent.objectId,
        objectSize = putEvent.objectSize,
        timestamp = putEvent.timestamp+10,
        serviceTimeNanos = 1,
        userId = putEvent.userId,
        correlationId = putEvent.correlationId,
      )
      objectId = putEvent.objectId
      selectedNodeId = putEvent.nodeId
      serviceTimeNanos = putEvent.serviceTimeNanos
      operationId      = putEvent.correlationId
      _                <- Events.saveEvents(
        events = List(putEvent,getEvent)
      )
      //      _                <- ctx.logger.info(s"PUT ${evictedEvent.nodeId} ${evictedEvent.objectId}")
      _  <- ctx.logger.info(s"UPLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
      response <- NoContent()
    } yield response
  }

}
