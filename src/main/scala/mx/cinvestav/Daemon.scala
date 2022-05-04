package mx.cinvestav

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import fs2.Stream
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{AddedNode, EventX, EventXOps, Get, Put, PutCompleted}
import mx.cinvestav.events.Events
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration
import mx.cinvestav.commons.types.{NodeX, PendingReplication, QueueInfo}
import mx.cinvestav.server.controllers.{DownloadControllerV2, UploadControllerV2}
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import java.util.UUID
import breeze._
import breeze.linalg.DenseVector
import breeze.stats.{mean, median}
import mx.cinvestav.commons.Implicits._

import scala.concurrent.duration._
import scala.language.postfixOps

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
            )(implicit ctx:NodeContext) =
    for {
      _               <- ctx.logger.debug(s"ACTIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")
      puts            = EventXOps.onlyPuts(events=events).asInstanceOf[List[Put]]
      maybeDefaultPut = puts.find(_.objectId == objectId)
      newPuts         = maybeDefaultPut match {
        case Some(defaultPut) =>
          selectedNodes.map{ node =>
            val (operationId,correlationId) = {
              val uuid = UUID.randomUUID().toString.substring(0, 5)
              (s"op-${uuid}",s"op-${uuid}_${defaultPut.getBlockIndex()}")
            }
            defaultPut.copy(
              nodeId           = node.nodeId,
              serviceTimeStart = serviceTimeStart,
              serviceTimeEnd   = serviceTimeEnd,
              serviceTimeNanos = serviceTimeEnd - serviceTimeStart,
              correlationId    = correlationId,
              arrivalTime      = arrivalTime,
              timestamp        = arrivalTime,
              operationId      = operationId
            )
          }
        case None => Nil
      }
//      _               <- ctx.logger.debug(s"REPLICA_PUTS $newPuts")
      _               <- Events.saveEvents(events = newPuts)
      operationIdsH   = newPuts.map{ x =>Header.Raw(CIString("Operation-Id"),x.operationId)}
      replicaNodeH    = selectedNodes.map(
          selectedNode => Header.Raw(CIString("Replica-Node"),selectedNode.nodeId),
        )
//      _ <-ctx.logger.debug("OPERATION_ID_HEADER "+operationIdsH.toString)
      activeRequest   = Request[IO](
        method  = Method.POST,
        uri     = Uri.unsafeFromString(s"http://${replicaNode.nodeId}:6666/api/v2/replicate/active/$objectId"),
        headers = Headers(operationIdsH).put(replicaNodeH)
      )
      activeResponse  <- ctx.client.status(activeRequest).onError{ e=>
        ctx.logger.error(e.getMessage)
      }
      res             = Option.when(activeResponse.isSuccess)(())
      _               <- ctx.logger.debug(activeResponse.toString+ s" <> $res")
    } yield res



  def apply(period:FiniteDuration)(implicit ctx:NodeContext): Stream[IO, Unit] = {
    val replicationDaemon = Stream.awakeEvery[IO](period).evalMap{ _ =>
      for {
        currentState       <- ctx.state.get
        pendingReplicas    = currentState.pendingReplicas
        rawEvents          = currentState.events
        events             = Events.orderAndFilterEventsMonotonicV2(rawEvents)
        nodexs             = EventXOps.getAllNodeXs(events = events)
        distributionSchema = Events.generateDistributionSchemaV3(events = events)
        completedPuts      = EventXOps.onlyPutCompleteds(events = events).asInstanceOf[List[PutCompleted]]
        completedPutsIds   = completedPuts.map(_.objectId)
        //                 _________________________________________________________________________________________________
        _ <- ctx.logger.debug(s"PENDING_REPLICAS $pendingReplicas")
//        _ <- ctx.logger.debug("COMPLETED_PUTS")
        ps                 <- pendingReplicas.toList.filter(x=>x._2.rf>0 && completedPutsIds.contains(x._1)).traverse{
          case (objectId,x) => for {
            _                     <- IO.unit
            objectId              = x.objectId
            rf                    = x.rf
            objectSize            = x.objectSize
            availableResourcesMap = nodexs.map(x=>x.nodeId->x).toMap
            fxReplicas            = distributionSchema.get(objectId).getOrElse(List.empty[String])
            _ <- ctx.logger.debug(s"REPLICAS_FX $fxReplicas")
            ps                    <- if(fxReplicas.isEmpty) IO.pure(Nil)
            else {
              for {
                _                 <- IO.unit
                fxReplicaNodes    = nodexs.filter(x=>fxReplicas.contains(x.nodeId))
                //          _________________________________________________________________________________________________
                availableNodexs   = nodexs.filterNot(x=>fxReplicas.contains(x.nodeId))
                //          _________________________________________________________________________________________________
                newRf             = {
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

                xxxx <- maybeAvailableNodexsNe match {
                  case Some(availableNodeXNEL) => for {
                    _                   <- IO.unit
                    serviceTimeStart    <- IO.monotonic.map(_.toNanos)
                    arrivalTime         <- IO.realTime.map(_.toNanos)
                    maybeSelectedNodes  = UploadControllerV2.balance(
                      events = events
                    )(
                      objectSize = objectSize,
                      nodes = availableNodeXNEL,
                      rf = x.rf
                    )
                    serviceTimeEnd      <- IO.monotonic.map(_.toNanos)
                    xxx                 <- maybeSelectedNodes match {
                      case Some(selectedNodes) => for {
                        _           <- ctx.logger.debug(s"SELECTED_NODES ${selectedNodes.length}")
                        replicaNode = fxReplicaNodes.head

                        xx  <- x.replicationTechnique match {
                          case "ACTIVE"                =>
                            for {
                              activeRes <- active(
                                  objectId         = objectId,
                                  x                = x,
                                  events           = events,
                                  selectedNodes    = selectedNodes,
                                  replicaNode      = replicaNode,
                                  serviceTimeStart = serviceTimeStart,
                                  serviceTimeEnd   = serviceTimeEnd,
                                  arrivalTime      = arrivalTime
                                ).onError{ e =>
                                  ctx.logger.error(e.getMessage)
                                }
                              newPendings = activeRes match {
                                case Some(_) => (objectId -> pendingReplication.copy(rf =  rf - selectedNodes.length  ))::Nil
                                case None => List.empty[(String,PendingReplication)]
                              }
                            } yield newPendings
                          case "PASSIVE"               => for {
                            _            <- ctx.logger.debug(s"PASSIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")
                            newPendings = Nil
                          } yield newPendings
                          case "COLLABORATIVE_PASSIVE" => for {
                            _ <- ctx.logger.debug(s"COLLABORATIVE_PASSIVE_REPLICATION ${x.rf} ${replicaNode.nodeId}")
                            newPendings = Nil
                          } yield newPendings
                        }
                        _  <- ctx.logger.debug(s"CREATE_REPLICAS $objectId $rf")
                        _  <- ctx.logger.debug("__________________________________________________")
                      } yield xx
                      case None => IO.pure(Nil)
                    }
                  } yield xxx
                  case None => ctx.logger.debug("NO AVAILABLE NODES") *> IO.pure(Nil)
                }
//                x =
              } yield xxxx
            }
          } yield ps
        }.map(_.flatten)
//
        _                  <- ctx.state.update { s =>
          s.copy(
            pendingReplicas = s.pendingReplicas ++ ps
          )
        }
//
      } yield()
    }.onError{ e=>
      ctx.logger.error(e.getMessage).pureS
    }

    val checkNodesDaemon = Stream.awakeEvery[IO](period = 15000 milliseconds).evalMap{ _ =>
      for{
        currentState    <- ctx.state.get
        rawEvents       = currentState.events
        events          = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        nodeIds         = Events.getNodeIds(events = events)

        queueInfoByNode = nodeIds.map(nodeId =>
          nodeId -> EventXOps.processQueueTimes(
            events = events,
            nodeFilterFn = _.nodeId == nodeId,
            mapArrivalTime = e => e match {
              case p:Put => p.arrivalTime
              case p:Get => p.arrivalTime
              case e     => e.monotonicTimestamp
            }
          ),
        ).toMap

        meanWaitingTimesMap = queueInfoByNode.map{
          case (str, info) =>  str -> info.global.meanWaitingTime
        }
        medianWaitingTimesMap = queueInfoByNode.map{
          case (str, info) =>  str -> info.global.medianWaitingTime
        }

        meanWaitingTimes        = DenseVector(meanWaitingTimesMap.values.toArray)
        meanMeanWaitingTime     = if(meanWaitingTimes.size == 0) 0.0 else mean(meanWaitingTimes)
        meanMedianWaitingTime   = if(meanWaitingTimes.size == 0) 0.0 else  median(meanWaitingTimes)
//
        medianWaitingTimes      = DenseVector(medianWaitingTimesMap.values.toArray)
        medianMeanWaitingTime   =if(medianWaitingTimes.size == 0) 0.0 else mean(medianWaitingTimes)
        medianMedianWaitingTime =if(medianWaitingTimes.size == 0) 0.0 else median(medianWaitingTimes)
//        _                       <- ctx.logger.debug(s"MEAN_MEAN $meanMeanWaitingTime")
//        _                       <- ctx.logger.debug(s"MEAN_MEAN $meanMedianWaitingTime")
//        _                       <- ctx.logger.debug(s"MEDIAN_MEAN $medianMeanWaitingTime")
//        _                       <- ctx.logger.debug(s"MEDIAN_MEDIAN $medianMedianWaitingTime")
//        _                       <- ctx.logger.debug("_____________________________________________")

      } yield ()
    }.onError{ e =>
      ctx.logger.error(e.getMessage).pureS
    }


    val systemReplication = Stream.awakeEvery[IO](period = 5 seconds).evalMap{ _ =>
      for {
        currentState   <- ctx.state.get
        pendingSysReps = currentState.pendingSystemReplicas
        rawEvents      = currentState.events
        events         = Events.orderAndFilterEventsMonotonicV2(rawEvents)
        currentAR      = EventXOps.onlyAddedNode(events = events).length
        maybePSR       = pendingSysReps.maxByOption(_.rf)
        _              <- maybePSR match {
          case Some(value) =>
            val diff = value.rf - currentAR
           val x =  if(diff > 0 ) (0 until diff).toList.traverse{_ =>
              ctx.config.systemReplication.createNode().flatMap(x=> ctx.logger.debug(s"CREATED_NODE ${x.nodeId}"))
            }
            else IO.unit
            ctx.logger.debug(s"DIFF_RF $diff") *> ctx.logger.debug(s"CURRENT_AR $currentAR") *> x
          case None => IO.unit
        }
        _ <- ctx.state.update{s=> s.copy(pendingSystemReplicas = Nil)}
      } yield ()
    }

    replicationDaemon concurrently checkNodesDaemon concurrently systemReplication
  }

}
