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
        currentState    <- ctx.state.get
        headers         = req.headers
        rf              = headers.get(CIString("RF")).map(_.head.value).flatMap(_.toIntOption).getOrElse(1)
        rawEvents       = currentState.events
        events          = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        maybeDumbObject = Events.getObjectByIdV2(objectId = objectId,events=events)
        response        <- maybeDumbObject match {
          case Some(dumbObject) => for {
            _               <- IO.unit
            replicaPuts     = Events.getObjectByIdInfo(objectId = objectId,events=events)
            _               <- ctx.logger.debug(s"REPLICA_PUTS $replicaPuts")
            replicaNodes    = replicaPuts.map(_.nodeId)
            nodeXs          = Events.getAllNodeXs(events = events).map(x=>x.nodeId -> x).toMap
            availableNodes  = nodeXs.filterNot{
              case (nodeId, _) => replicaNodes.contains(nodeId)
            }
            ar              = availableNodes.size
            rfDiff          = ar - rf
            _               <- if(rfDiff < 0) {
              for {
                _ <- ctx.logger.debug(s"CREATE_NODES ${rfDiff*(-1)}")
              } yield ()
            } else {
              for {
                _ <- ctx.logger.debug(s"REPLICATE $rf ${}")
              } yield ()
            }
            res <- Accepted()
          } yield res
//        _________________________________________________________________________________________________
          case None => Forbidden()
        }
      } yield response
    }

  }

}
