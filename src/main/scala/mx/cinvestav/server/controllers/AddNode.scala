package mx.cinvestav.server.controllers

import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.commons.events.ServiceReplicator.AddedService
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

object AddNode {

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root / "nodes" / "add"  => for {
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      rawEvents        = currentState.events
      events           = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
      nodes            = Events.onlyAddedNode(events=events)
      maxAR            = ctx.config.availableResources
      response         <- if(nodes.length < maxAR) for {
        payload        <- req.as[AddedService]
        eventId        = UUID.randomUUID()
        headers        = req.headers
        timestamp      = headers.get(CIString("Timestamp")).flatMap(_.head.value.toLongOption)
        operationId    = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        latency        = timestamp.map(arrivalTime - _)
        serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
        newEvent         =  AddedNode(
          serialNumber = 0,
          nodeId = ctx.config.nodeId,
          addedNodeId = payload.nodeId,
          ipAddress = payload.hostname,
          port      = payload.port,
          totalStorageCapacity = payload.totalStorageCapacity,
          totalMemoryCapacity =payload.totalMemoryCapacity,
          cacheSize = payload.cacheSize,
          cachePolicy = payload.cachePolicy,
          timestamp = arrivalTime,
          serviceTimeNanos =serviceTimeNanos,
          correlationId = operationId,
          monotonicTimestamp = 0L
        )
        _                <- Events.saveEvents(events =newEvent ::Nil)
        response         <- NoContent()
      } yield response
      else NoContent()
    } yield  response
  }

}
