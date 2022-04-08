package mx.cinvestav

import cats.implicits._
import cats.effect.IO
import fs2.Stream
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.events.Events
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.types.PendingReplication
import mx.cinvestav.server.controllers.DownloadControllerV2

object Daemon {
  def apply(period:FiniteDuration)(implicit ctx:NodeContext): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](period).evalMap{ _ =>
      for {
        currentState    <- ctx.state.get
        pendingReplicas = currentState.pendingReplicas
//        maybePending    = pendingReplicas.headOption
        ps              <- pendingReplicas.toList.filter(_._2.rf>0).traverse{
          case (objectId,x) => for {
            _                  <- IO.unit
            objectId           = x.objectId
            rf                 = x.rf
            objectSize         = x.objectSize
            rawEvents          = currentState.events
            events             = Events.orderAndFilterEventsMonotonicV2(rawEvents)
            nodexs             = EventXOps.getAllNodeXs(events = events)
            arMap              = nodexs.map(x=>x.nodeId->x).toMap
            ds                 = Events.generateDistributionSchema(events = events)
            replicaNodexIds    = ds.get(objectId).getOrElse(List.empty[String])
            replicaNodexs      = nodexs.filter(x=>replicaNodexIds.contains(x.nodeId))
            availableNodexs    = nodexs.filterNot(x=>replicaNodexIds.contains(x.nodeId))
            newRf              = {
              val x0 = rf - availableNodexs.length
              if(x0<0) 0 else x0
            }
            pendingReplication = PendingReplication(rf = newRf,objectId = objectId, objectSize = objectSize)
            _                  <- ctx.logger.debug(s"CREATE_REPLICAS $objectId $rf")
//            xs  = DownloadControllerV2.balance(locations = replicaNodexIds,arMap = arMap,objectSize = objectSize, gets = gets)

            _                 <- ctx.logger.debug("__________________________________________________")
          } yield (objectId-> pendingReplication)
//          case None => IO.unit
        }
      } yield()
    }
  }

}
