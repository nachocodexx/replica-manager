package mx.cinvestav.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.commons.types.NodeX

import java.util.UUID
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
//
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
//
import org.typelevel.ci.CIString
import mx.cinvestav.commons.payloads.AddCacheNode

object AddNode {
//  case class Payload(nodeId:String, ip:String, port:Int, metadata:Map[String,String])

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root  => for {
      arrivalTime  <- IO.realTime.map(_.toMillis)
      payload      <- req.as[AddCacheNode]
      eventId      = UUID.randomUUID()
      headers      = req.headers
      timestamp    = headers.get(CIString("Timestamp"))
        .flatMap(_.head.value.toLongOption)
      latency      = timestamp.map(arrivalTime - _)
//    __________________________________________________
      newNode = payload.nodeId ->  NodeX(
        nodeId = payload.nodeId,
        ip = payload.ip,
        port = payload.port,
        totalStorageCapacity =payload.totalStorageCapacity,
        availableStorageCapacity = payload.availableStorageCapacity,
        usedStorageCapacity = 0L,
        //
        availableCacheSize= payload.cacheSize,
        cacheSize =payload.cacheSize,
        usedCacheSize = 0,
        cachePolicy = payload.cachePolicy,
        metadata = payload.metadata
      )
      _ <- ctx.logger.debug(s"NEW_NODE ${newNode._2.asJson}")
      serviceTime <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
      _            <- ctx.state.update{ state =>
        val addedNode  = AddedNode(
          eventId = UUID.randomUUID().toString,
          serialNumber = state.events.length,
          nodeId = ctx.config.nodeId,
          addedNodeId = payload.nodeId,
          ipAddress = payload.ip,
          port      = payload.port,
          totalStorageCapacity = payload.totalStorageCapacity,
          cacheSize = payload.cacheSize,
          cachePolicy = payload.cachePolicy,
          timestamp = arrivalTime,
          eventType = "ADDED_NODE",
          milliSeconds =serviceTime
        )
        state.copy(
          //          AR =  state.AR + newNode ,
          events = state.events :+ addedNode
        )
      }
//
      _           <- ctx.logger.info(s"ADD_NODE_LATENCY ${latency.getOrElse(0)}")
      _           <- ctx.logger.info(s"ADD_NODE $eventId $serviceTime")
      response    <- Ok("ADD_NODE")
    } yield  response
  }

}
