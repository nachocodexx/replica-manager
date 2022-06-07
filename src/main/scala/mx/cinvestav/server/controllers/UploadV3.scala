package mx.cinvestav.server.controllers

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.typelevel.ci.CIString
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.commons.types.NodeWhat
import mx.cinvestav.operations.Operations.{ProcessedUploadRequest, nextOperation}
//
import scala.concurrent.duration._
import language.postfixOps

//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.types.{Download, NodeBalance, NodeReplicationSchema, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadBalance, UploadCompleted, UploadRequest, UploadResult, What}
import mx.cinvestav.events.Events
import mx.cinvestav.commons.events.{EventX, EventXOps}
import mx.cinvestav.commons.{types, utils}
import mx.cinvestav.operations.Operations
//import mx.cinvestav.commons.utils


object UploadV3 {


  def elasticity(payload:UploadRequest,nodes:Map[String,NodeX])(implicit ctx:NodeContext) = {
    val nodeIdIO = if(payload.elastic){
      val maxRF = payload.what.map(_.metadata.getOrElse("REPLICATION_FACTOR","1").toInt).max
      if(maxRF > nodes.size) {
        val xs = (0 until  (maxRF-nodes.size) ).toList.traverse{ index=>
          val x =  if(ctx.config.elasticityTime == "REACTIVE")  {
            val nrs = NodeReplicationSchema.empty(id = "")
            val app = ctx.config.systemReplication.createNode(nrs = nrs)
            app.map(_.nodeId)
          } else   {
            val id = utils.generateStorageNodeId(autoId = true)
            val nrs = NodeReplicationSchema.empty(id =  id)
            val app = ctx.config.systemReplication.createNode(nrs = nrs)
            app.start.map(_=> id)
          }
          x
        }
        xs
      } else IO.pure(Nil)
    }
    else IO.pure(List.empty[String])


    nodeIdIO.map(nodeIds =>  nodeIds.map(nodeId => NodeX.empty(nodeId = nodeId) ))
  }



  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{

    case req@POST -> Root / "upload" =>
      val app = for {
//    _____________________________________________________________________
      _                  <- s.acquire
      currentState       <- ctx.state.get
      headers            = req.headers
      clientId           = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
      arrivalTime        <- IO.monotonic.map(_.toNanos)
      payload            <- req.as[UploadRequest]
      newNodes           <- elasticity(payload,nodes= currentState.nodes).map(ns=> ns.map(n=>n.nodeId -> n).toMap )
//    __________________________________________________________________________________________________________________
      _                  <- ctx.state.update{s=> s.copy(nodes = s.nodes ++ newNodes)}
      currentState       <- ctx.state.get
//    __________________________________________________________________________________________________________________
      avgServiceTimes    = Operations.getAVGServiceTime(operations= currentState.completedOperations)
//      avgWaitingTimes    = Operations.getAVGWaitingTimeByNode(completedOperations= currentState.completedQueue,queue = currentState.nodeQueue)
      nodexs             = currentState.nodes
      _ <-ctx.logger.debug(s"NODEXS ${nodexs.keys}")
//      ar                 = nodexs.size
//    __________________________________________________________________________________________________________________
//      _                  <- ctx.logger.debug("AVG_ST "+avgServiceTimes.asJson.toString)
      pur                = Operations.processUploadRequest(operations = currentState.operations)(ur = payload,nodexs = nodexs)
      _                  <- ctx.state.update{ s=>
        s.copy(
          nodes = pur.nodexs
        )
      }
      newOps             <- pur.rss.traverse(rs=>Operations.processRSAndUpdateQueue(clientId = clientId)( rs = rs)).map(_.flatten)
      _ <- ctx.state.update{s=>s.copy(operations = s.operations ++ newOps)}
      xsGrouped          = newOps.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }
//      EXTRA SERVICE TIME
      extra = scala.util.Random.nextLong(ctx.config.extraServiceTimeMs) + scala.util.Random.nextLong(1000L)+100
      _                 <- IO.sleep(extra milliseconds)
//
      serviceTime        <- IO.monotonic.map(_.toNanos - arrivalTime)
      id                 = utils.generateNodeId(prefix = "op",autoId=true)
      uploadRes          <- Operations.generateUploadBalance(xs = xsGrouped)
        .map{_.copy(serviceTime = serviceTime,id=id)}
      response           <- Ok(uploadRes.asJson)
      _                  <- s.release
    } yield response
      app.onError{ e=>
        ctx.logger.error(e.getMessage)
      }
    case req@POST -> Root / "upload" /"completed" => for{
      serviceTimeStart <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      headers          = req.headers
      nodeId           = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("NODE_ID")
      objectId         = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse("")
      operationId      = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse("")
//      objectSize       = headers.get(CIString("Object-Size")).map(_.head.value).getOrElse("")
      queue            = currentState.nodeQueue
      nodeQueue        = queue.getOrElse(nodeId,Nil).sortBy(_.serialNumber)
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
//      maybeUp          = nodeQueue.find(_.operationId == operationId)
      maybeUp          = currentState.operations.find(_.operationId == operationId)

      _ <- ctx.logger.debug(s"OBJECT_ID $objectId")
      _ <- ctx.logger.debug(s"OPERATION_ID $operationId")
      _ <- ctx.logger.debug(maybeUp.toString)
      _ <- ctx.logger.debug("________________________________")

      lastCompleted    = completeQueue.maxByOption(_.arrivalTime)

      response         <- maybeUp match {
        case Some(o) =>
          o match {
            case up:Upload =>
              val wt  = lastCompleted.map(_.departureTime).getOrElse(0L) - up.arrivalTime
              val completed  = UploadCompleted(
                operationId  = up.operationId,
                serialNumber = up.serialNumber,
                arrivalTime  = serviceTimeStart,
                serviceTime  = serviceTimeStart - up.arrivalTime,
                waitingTime  = if(wt < 0L ) 0L else wt,
                idleTime     = if(wt < 0L) wt*(-1) else 0L,
                objectId     = objectId,
                nodeId       = nodeId,
                metadata     = Map.empty[String,String] ++ up.metadata,
                objectSize   = up.objectSize
              )
              val nextOp    = nodeQueue.filter(_.operationId != operationId).minByOption(_.serialNumber)
               val saveOp = ctx.state.update{ s=>
                 s.copy(
                   completedQueue      =  s.completedQueue.updatedWith(nodeId) {
                     case op@Some(value) =>
                       op.map(x => x :+ completed)
                     case None => (completed :: Nil).some
                   },
                   completedOperations =  s.completedOperations :+completed,
                   pendingQueue        =  s.pendingQueue.updated(nodeId,nextOp),
                   nodeQueue           =  s.nodeQueue.updated(
                     nodeId,
                     s.nodeQueue.getOrElse(nodeId,Nil).filterNot(_.operationId == operationId )
                   )
                 )
               }
              for {
                _   <- saveOp
                res <- NoContent()
              } yield res
            case _ => NotFound()
          }
        case None => NotFound()
      }
    } yield response

    case req@POST -> Root /"foo"/ "upload" =>
      for {
        currentState <- ctx.state.get
        rawEvents    = currentState.events
        events       = Events.orderAndFilterEventsMonotonicV2(events = rawEvents)
        nodexs       = EventXOps.getAllNodeXs(events = events).map(n=>n.nodeId -> n).toMap
        arrivalTime  <- IO.monotonic.map(_.toNanos)
        headers      = req.headers
        clientId     = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        payload      <- req.as[ReplicationSchema]
//        nrss         = payload.nodes
//        _            <- nrss.traverse{ nrs=>
//          ctx.config.systemReplication.createNode(nrs =nrs)
//        }.start
//        res <- genQueue(events = events, payload = payload,nodexs=nodexs)
//        _ <- res match {
//          case Left(value) => ctx.logger.error(value.toString)
//          case Right(value) =>
//            ctx.logger.debug(value.toString)
//        }
        response     <- Ok()
      } yield response
  }

}
