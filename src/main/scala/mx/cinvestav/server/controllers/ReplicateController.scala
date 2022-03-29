package mx.cinvestav.server.controllers

import cats.effect._
import cats.implicits._
import mx.cinvestav.commons.events.Put
import org.typelevel.ci.CIString

import java.util.UUID
//
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.events.Events
//
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object ReplicateController {


  def apply()(implicit ctx:NodeContext): HttpRoutes[IO] = {

    HttpRoutes.of[IO]{
      case req@GET -> Root / "replicate" / objectId => for {
//
        currentState  <- ctx.state.get
        headers       = req.headers
        rf            = headers.get(CIString("RF")).map(_.head.value).flatMap(_.toIntOption).getOrElse(1)
        rawEvents     = currentState.events
        events        = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        dumbObject    = Events.getObjectByIdV2(objectId = objectId,events=events)
        replicaPuts   = Events.getObjectByIdInfo(objectId = objectId,events=events)
        _             <- ctx.logger.debug(s"REPLICA_PUTS $replicaPuts")
        replicaNodes  = replicaPuts.map(_.nodeId)
        nodeXs        = Events.getAllNodeXs(events = events).map(x=>x.nodeId -> x).toMap

        //        maybeNodeX   = Events.onlyPutos(events=events).map(_.asInstanceOf[Put]).filter(_.objectId == objectId).map{ x=>
        //          val nodeId = x.nodeId
        //          val nodeX  = nodeXs.get(nodeId)
        //          nodeX
        //        }.filter(_.isDefined).map(_.get)
        //        replicaIndex = (0 until rf).toList
        ////        _ <-
        ////        xs = if(rf > maybeNodeX.length) replicaIndex.zipWithIndex.map( x => (x._1, maybeNodeX(x._2 % maybeNodeX.length)) )
        ////        else maybeNodeX.zipWithIndex.map( x => (x._1, replicaIndex(x._2 % replicaIndex.length)) )
        //        _            <- maybeObject match {
        //          case Some(dumbObject) => for {
        //            _           <- IO.unit
        //            objectSize  = dumbObject.objectSize
        //            now         <- IO.realTime.map(_.toMillis)
        //            operationId = UUID.randomUUID().toString
        //
        ////
        //          } yield ()
        //          case None => IO.unit
        //        }
        //
        response     <- Ok()
      } yield response
    }

  }

}
