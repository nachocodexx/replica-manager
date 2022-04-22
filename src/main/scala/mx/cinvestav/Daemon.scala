package mx.cinvestav

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import fs2.Stream
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{AddedNode, EventX, EventXOps, Put}
import mx.cinvestav.events.Events
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration
import mx.cinvestav.commons.types.{NodeX, PendingReplication}
import mx.cinvestav.server.controllers.{DownloadControllerV2, UploadControllerV2}
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import java.util.UUID

object Daemon {


  def active(
              objectId:String,
              x:PendingReplication,
              events:List[EventX],
              selectedNodes:List[NodeX],
              replicaNode:NodeX,
              serviceTimeStart:Long = 0L,
              serviceTimeEnd:Long   = 0L,
              arrivalTime:Long      = 0L
            )(implicit ctx:NodeContext) = for {
      _               <- ctx.logger.debug(s"ACTIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")

      puts = EventXOps.onlyPuts(events=events).asInstanceOf[List[Put]]
       _ <- ctx.logger.debug(s"PUTS ${puts.toString()}")
      maybeDefaultPut = puts.find(_.objectId == objectId)
      _ <- ctx.logger.debug(s"DEFAULT_PUT ${maybeDefaultPut}")
      //                    __________________________________________________________
      newPuts         = maybeDefaultPut match {
        case Some(defaultPut) =>
          selectedNodes.map{ node =>
            val operationId = {
              val uuid = UUID.randomUUID().toString.substring(0, 5)
              s"op-$uuid"
            }
            defaultPut.copy(
              nodeId           = node.nodeId,
              serviceTimeStart = serviceTimeStart,
              serviceTimeEnd   = serviceTimeEnd,
              serviceTimeNanos = serviceTimeEnd - serviceTimeStart,
              correlationId    = operationId,
              arrivalTime      = arrivalTime,
              timestamp        = arrivalTime
            )
          }
        case None => Nil
      }
      //                    __________________________________________________________
      _ <- Events.saveEvents(events = newPuts)
      activeRequest = Request[IO](
        method = Method.POST,
        uri    = Uri.unsafeFromString(s"http://${replicaNode.nodeId}:6666/api/v2/replicate/active/$objectId"),
        headers = Headers(
          selectedNodes.map(
            selectedNode => Header.Raw(CIString("Replica-Node"),selectedNode.nodeId)
          )
        )
      )
      activeResponse <- ctx.client.status(activeRequest).onError{ e=>
        ctx.logger.error(e.getMessage)
      }
      _ <- ctx.logger.debug(activeResponse.toString)
    } yield ()



  def apply(period:FiniteDuration)(implicit ctx:NodeContext): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](period).evalMap{ _ =>
      for {
        currentState    <- ctx.state.get
        pendingReplicas = currentState.pendingReplicas
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
            ds                 = Events.generateDistributionSchemaV3(events = events)
            replicaNodexIds    = ds.get(objectId).getOrElse(List.empty[String])
            replicaNodexs      = nodexs.filter(x=>replicaNodexIds.contains(x.nodeId))
//          _________________________________________________________________________________________________
            availableNodexs    = nodexs.filterNot(x=>replicaNodexIds.contains(x.nodeId))
//          _________________________________________________________________________________________________
            newRf              = {
              val x0 = rf - availableNodexs.length
              if(x0<0) 0 else x0
            }
            pendingReplication = PendingReplication(
              rf = newRf,
              objectId = objectId,
              objectSize = objectSize
            )
//          _________________________________________________________________________________________________
            maybeAvailableNodexsNe  = NonEmptyList.fromList(availableNodexs)

            _ <- maybeAvailableNodexsNe match {
              case Some(value) => for {
                _                <- IO.unit
                serviceTimeStart <- IO.monotonic.map(_.toNanos)
                arrivalTime      <- IO.realTime.map(_.toNanos)
                maybeSelectedNodes  = UploadControllerV2.balance(
                events = events
                )(
                  objectSize = objectSize,
                  nodes = value,
                  rf = x.rf
                )
                serviceTimeEnd <- IO.monotonic.map(_.toNanos)

                _           <- maybeSelectedNodes match {
                  case Some(selectedNodes) => for {
                    _           <- ctx.logger.debug(s"SELECTED_NODES ${selectedNodes.length}")
                    replicaNode = replicaNodexs.head

                    _  <- x.replicationTechnique match {
                      case "ACTIVE"                => active(
                        objectId         = objectId,
                        x                = x,
                        events           = events,
                        selectedNodes    = selectedNodes,
                        replicaNode      = replicaNode,
                        serviceTimeStart = serviceTimeStart,
                        serviceTimeEnd   = serviceTimeEnd,
                        arrivalTime      = arrivalTime
                      )
                      case "PASSIVE"               => for {
                        _ <- ctx.logger.debug(s"PASSIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")
                      } yield ()
                      case "COLLABORATIVE_PASSIVE" => for {
                        _ <- ctx.logger.debug(s"COLLABORATIVE_PASSIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")
                      } yield ()
                    }
                    _  <- ctx.logger.debug(s"CREATE_REPLICAS $objectId $rf")
                    _  <- ctx.logger.debug("__________________________________________________")
                  } yield ()
                  case None => IO.unit
                }
              } yield ()
              case None => IO.unit
            }
          } yield (objectId-> pendingReplication)
        }
        _ <- ctx.state.update(s=>s.copy(pendingReplicas = ps.toMap))
      } yield()
    }
  }

}
