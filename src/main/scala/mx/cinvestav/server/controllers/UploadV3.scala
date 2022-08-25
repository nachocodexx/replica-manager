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
//import mx.cinvestav.commons.types.NodeWhat
//import mx.cinvestav.operations.Operations.{ProcessedUploadRequest, nextOperation}
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
        arrivalTime        <- IO.monotonic.map(_.toNanos)
        _                  <- s.acquire
        currentState       <- ctx.state.get
        headers            = req.headers
        clientId           = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        payload            <- req.as[UploadRequest]
        startElasticity    <- IO.monotonic.map(_.toNanos)
        newNodes           <- elasticity(payload,nodes= currentState.nodes).map(ns=> ns.map(n=>n.nodeId -> n).toMap )
        elasticitySt       <- IO.monotonic.map(_.toNanos).map(_ - startElasticity)
        _                  <- if(ctx.config.elasticity) ctx.logger.info(s"ELASTICITY_ST $elasticitySt") else IO.unit

        //    __________________________________________________________________________________________________________________
        _                  <- ctx.state.update{s=> s.copy(nodes = s.nodes ++ newNodes)}
        currentState       <- ctx.state.get
        //    __________________________________________________________________________________________________________________
        //      avgServiceTimes    = Operations.getAVGServiceTime(operations= currentState.completedOperations)
        //      avgWaitingTimes    = Operations.getAVGWaitingTimeByNode(completedOperations= currentState.completedQueue,queue = currentState.nodeQueue)
        nodexs             = currentState.nodes
        //    __________________________________________________________________________________________________________________
        pur                = Operations.processUploadRequest(operations = currentState.operations)(ur = payload,nodexs = nodexs)
        _                  <- ctx.state.update{ s=>
        s.copy(
          nodes = pur.nodexs
        )
      }
        newOps             <- pur.rss.traverse(rs=>Operations.processRSAndUpdateQueue(clientId = clientId)( rs = rs)).map(_.flatten)
        _                  <- ctx.state.update{s=>s.copy(operations = s.operations ++ newOps)}
        xsGrouped          = newOps.groupBy(_.nodeId).map{
        case (nId,ops)=> nId -> ops.sortBy(_.serialNumber)
      }
        //      EXTRA SERVICE TIME
        extra             = if (ctx.config.extraServiceTimeMs <= 1) 0 else (scala.util.Random.nextLong(ctx.config.extraServiceTimeMs*10) + ctx.config.extraServiceTimeMs)
        _                 <- IO.sleep(extra milliseconds)
        //
        //    ___________________________________________________________________________
        serviceTime        <- IO.monotonic.map(_.toNanos - arrivalTime)
        id                 = utils.generateNodeId(prefix = "op",autoId=true,len=10)
        //      uploadRes          <- Operations.generateUploadBalance(xs = xsGrouped).map{_.copy(serviceTime = serviceTime,id=id)}
        uploadRes          <- Operations.generateUploadBalance(xs = xsGrouped).map{_.copy(id=id,serviceTime = serviceTime)}
        response           <- Ok(uploadRes.asJson)
        serviceTime        <- IO.monotonic.map(_.toNanos - arrivalTime)
        _                  <- uploadRes.result.toList.traverse{
        case (objectId, x) =>
          x.results.traverse{ d =>
            val operationId   = d.operationId
            val objectSize    = d.objectSize
            val st            = serviceTime
            val avgST         = d.avgServiceTime
            val wt            = d.avgWaitingTime
            val nodeId        = d.nodeId
            ctx.logger.info(s"UPLOAD $operationId $objectId $objectSize $nodeId $st $avgST $wt") *> Operations.nodeNextOperation(nodeId).start.void
          }
      }
        _                  <- ctx.logger.debug("_____________________________________________")
        _                  <- s.release
//        _
//      _                  <- Operations.nextOperationV2().start
      } yield response
      app.onError{ e=>
        ctx.logger.error(e.getMessage)
      }
    case req@POST -> Root / "upload" /"completed" => for{
      serviceTimeStart <- IO.monotonic.map(_.toNanos)
      _                <- s.acquire
      currentState     <- ctx.state.get
      headers          = req.headers
      nodeId           = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("NODE_ID")
      objectId         = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse("")
      operationId      = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse("")
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
      maybeUp          = currentState.operations.find(_.operationId == operationId)

      lastCompleted    = completeQueue.maxByOption(_.arrivalTime)
      response         <- maybeUp match {
        case Some(o) =>
          o match {
            case up:Upload =>
              val _wt       = lastCompleted.map(_.departureTime).getOrElse(0L) - up.arrivalTime
              val wt        = if(_wt > 0) _wt else 0
              val completed = UploadCompleted(
                operationId  = up.operationId,
                serialNumber = up.serialNumber,
                arrivalTime  = up.arrivalTime,
                serviceTime  = serviceTimeStart - up.arrivalTime,
                waitingTime  = if(wt < 0L ) 0L else wt,
                idleTime     = if(wt < 0L) wt*(-1) else 0L,
                objectId     = objectId,
                nodeId       = nodeId,
                metadata     = Map.empty[String,String] ++ up.metadata,
                objectSize   = up.objectSize
              )
              val saveOp    = ctx.state.update{ s=>
                 s.copy(
                   completedQueue      =  s.completedQueue.updatedWith(nodeId) {
                     case op@Some(value) =>
                       op.map(x => x :+ completed)
                     case None => (completed :: Nil).some
                   },
                   completedOperations =  s.completedOperations :+completed,
                   pendingQueue        =  s.pendingQueue.updatedWith(nodeId) {
                     case Some(value) => None
                     case None => None
                   },
                   nodeQueue           =  s.nodeQueue.updated(
                     nodeId,
                     s.nodeQueue.getOrElse(nodeId,Nil).filterNot(_.operationId == operationId )
                   )
                 )
              }
//            _______________________________________________________________________________________________________
              for {
                _          <- IO.unit
                objectSize = up.objectSize
                nodeId     = up.nodeId
                st         = completed.serviceTime
                _          <- ctx.logger.info(s"UPLOAD_COMPLETED $operationId $objectId $objectSize $nodeId $st 0 $wt")
                _          <- saveOp
//                PUBLISH TO ALL CLIENTS
                publishToClients = currentState.clients.distinct.traverse(_.publish(objectId = objectId, objectSize = up.objectSize)).start.void
                _   <- ctx.config.replicationTechnique match {
                  case "ACTIVE" =>
                    for {
                      currentState      <- ctx.state.get
                      allQOperations    = currentState.nodeQueue.values.toList.flatten
                      pendingUps        = Operations.onlyUpload(operations = allQOperations).asInstanceOf[List[Upload]]
                      pendingUpsByObjId = pendingUps.groupBy(_.objectId)
                      currentPendingUps = pendingUpsByObjId.getOrElse(objectId,Nil).filter(_.operationId != operationId)
                      _                 <- if(currentPendingUps.isEmpty) publishToClients else IO.unit
                      _                 <- ctx.logger.debug(s"PENDING_REPLICAS $objectId ${currentPendingUps.length}")

                    } yield ()
//                _________________________________________
                  case "PASSIVE" => publishToClients
                  case _         => publishToClients
                }
                res <- NoContent()
              } yield res
            case _ => NotFound()
          }
        case None => NotFound()
      }
      _                <- s.release
      _                <- Operations.nodeNextOperation(nodeId)
      _                <- ctx.logger.debug("_____________________________________________")
    } yield response
  }

}
