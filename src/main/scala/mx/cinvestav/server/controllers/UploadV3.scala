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

//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.types.{Download, NodeBalance, NodeReplicationSchema, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadBalance, UploadCompleted, UploadRequest, UploadResult, What}
import mx.cinvestav.events.Events
import mx.cinvestav.commons.events.{EventX, EventXOps}
import mx.cinvestav.commons.{types, utils}
import mx.cinvestav.operations.Operations
//import mx.cinvestav.commons.utils


object UploadV3 {

  def elasticity(payload:UploadRequest)(implicit ctx:NodeContext) = {
    if(payload.elastic){
      val leftNodes = payload.what.map(_.metadata.getOrElse("REPLICATION_FACTOR","1").toInt).max
      val xs = (0 until leftNodes).toList.traverse{ index=>
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
    }
    else IO.pure(List.empty[String])
  }

  def sendRP(rp:Map[String,ReplicationProcess])(implicit ctx:NodeContext) = {
    for{
      _ <- IO.unit
    } yield ()
  }


  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{

    case req@GET -> Root / "upload" =>
      val app = for {
//    _____________________________________________________________________
      _                  <- s.acquire
      headers            = req.headers
      clientId           = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
      technique          = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
      arrivalTime        <- IO.monotonic.map(_.toNanos)
      payload            <- req.as[UploadRequest]

      newNodes           <- elasticity(payload)
//    __________________________________________________________________________________________________________________
      currentState       <- ctx.state.get
      nodesQueue         = currentState.nodeQueue
      avgServiceTimes    = Operations.getAVGServiceTime(operations= currentState.completedOperations)
      nodexs             = currentState.nodes
      ar                 = nodexs.size
//      ds                 = Operations.distributionSchema(operations = Nil,completedOperations = currentState.completedOperations,queue = nodesQueue,technique =technique)

//    __________________________________________________________________________________________________________________
      _                  <- ctx.logger.debug(avgServiceTimes.asJson.toString)
      (nodes,rss)        = Operations.processUploadRequest(operations = currentState.operations)(ur = payload,nodexs = nodexs)
//
      _                  <- ctx.state.update{ s=>
        s.copy(
          nodes = nodes
        )
      }

      newOps                <- rss.traverse(rs=>Operations.processRSAndUpdateQueue(clientId = clientId)( rs = rs)).map(_.flatten)
      xsGrouped    = newOps.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }

      serviceTime        <- IO.monotonic.map(_.toNanos - arrivalTime)
      id                 = utils.generateNodeId(prefix = "op",autoId=true)
      uploadRes          <- Operations.generateUploadBalance(xs = xsGrouped).map(_.copy(serviceTime = serviceTime,id=id))
      response           <- Ok(uploadRes.asJson)
      _                  <- s.release
      _ <- rss.traverse{ rs =>
        rs.data.headOption match {
          case Some(value) => ???
          case None => IO.unit
        }
      }
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
      queue            = currentState.nodeQueue
      nodeQueue        = queue.getOrElse(nodeId,Nil)
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
      maybeUp          = nodeQueue.find(_.operationId == operationId)
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
                metadata      = Map.empty[String,String],
              )
              val nextOp    = nodeQueue.headOption
               val saveOp = ctx.state.update{ s=>
                 s.copy(
                   completedQueue      =  s.completedQueue.updatedWith(nodeId){
                     op=>
                       op match {
                         case Some(value) =>
                           op.map( x => x :+ completed )
                         case None => (completed::Nil).some
                       }
                   },
                   completedOperations =  s.completedOperations :+completed,
                   pendingQueue        =  s.pendingQueue.updated(nodeId,nextOp),
                   nodeQueue           =  s.nodeQueue.updated(nodeId,s.nodeQueue.getOrElse(nodeId,Nil).filterNot(_.operationId == operationId ))
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
    case req@POST -> Root / "upload" =>
//      val
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
