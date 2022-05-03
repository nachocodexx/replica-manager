package mx.cinvestav.server.controllers

import cats.effect._
import cats.implicits._
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.PendingReplication
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
//
        headers         = req.headers
        rf              = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(1)
        replicationTech = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse("ACTIVE")
        replicationType = headers.get(CIString("Replication-Type")).map(_.head.value).getOrElse("PUSH")
//
        rawEvents       = currentState.events
        events          = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        maybeDumbObject = Events.getObjectByIdV2(objectId = objectId,events=events)

        response        <- maybeDumbObject match {
          case Some(dumbObject) => for {
            _                      <- IO.unit
            objectSize             = dumbObject.objectSize
            replicaPuts            = Events.getObjectByIdInfo(objectId = objectId,events=events)
            _                      <- ctx.logger.debug(s"REPLICA_PUTS $replicaPuts")
            replicaNodes           = replicaPuts.map(_.nodeId)
            nodeXs                 = Events.getAllNodeXs(events = events).map(x=>x.nodeId -> x).toMap
            availableReplicaNodes  = nodeXs.filterNot{
              case (nodeId, _) => replicaNodes.contains(nodeId)
            }
            ar                     = availableReplicaNodes.size
            rfDiff                 = ar - rf
            //
            _               <- if(rfDiff < 0) {
              for {
                _  <- IO.unit
                x  = rfDiff*(-1)
                _  <- ctx.logger.debug(s"CREATE_NODES ${}")
                xs <- ctx.config.systemReplication.launchNode().replicateA(x).start
              } yield ()
            }
//
            else IO.unit
            _   <- ctx.state.update{ s =>
              val newReplicas = Map(
                objectId -> PendingReplication(objectId = objectId, objectSize = objectSize, rf = rf, replicationTechnique = replicationTech)
              )
              s.copy(pendingReplicas =  s.pendingReplicas ++ newReplicas)
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
