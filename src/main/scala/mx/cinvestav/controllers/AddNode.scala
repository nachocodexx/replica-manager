package mx.cinvestav.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.{NodeContext, NodeX}

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

object AddNode {
  case class Payload(nodeId:String, ip:String, port:Int, metadata:Map[String,String])

  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST  -> Root  => for {
      arrivalTime  <- IO.realTime.map(_.toMillis)
      payload      <- req.as[Payload]
      eventId      = UUID.randomUUID()
      headers      = req.headers
      timestamp    = headers.get(CIString("Timestamp")).flatMap(_.head.value.toLongOption)
      latency      = timestamp.map(arrivalTime - _)
//    __________________________________________________
      _            <- ctx.state.update{ state =>
        val newNode = payload.nodeId ->  NodeX(
          nodeId = payload.nodeId,
          ip = payload.ip,
          port = payload.port,
          metadata = payload.metadata
//            Map.empty[String,String]
        )
        state.copy(AR =  state.AR + newNode )
      }
      serviceTime <- IO.realTime.map(_.toMillis).map( arrivalTime - _)
//
      _           <- ctx.logger.info(s"LATENCY $latency")
      _           <- ctx.logger.info(s"ADD_NODE $eventId $serviceTime")
      response    <- Ok("ADD_NODE")
    } yield  response
  }

}
