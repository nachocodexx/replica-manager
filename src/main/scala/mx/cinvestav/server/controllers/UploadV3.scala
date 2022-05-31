package mx.cinvestav.server.controllers

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.NodeContext
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import mx.cinvestav.commons.types.{Download, NodeBalance, NodeReplicationSchema, NodeX, Operation, ReplicationProcess, ReplicationSchema, Upload, UploadBalance, UploadCompleted, What,UploadRequest}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.events.Events
import mx.cinvestav.commons.events.{EventX, EventXOps}
import org.typelevel.ci.CIString
import mx.cinvestav.commons.{types, utils}
import mx.cinvestav.operations.Operations
//import mx.cinvestav.commons.utils


object UploadV3 {
  trait ErrorX {
    def msg:String
  }
  case class NoDefinedNodes(nodeIds:List[String]) extends ErrorX {
    override def  msg = s"NO_DEFINED_NODES $nodeIds"
  }
  type QueueBalancing = Map[ReplicationProcess, Map[What,List[String]]]
  type GenerateQueue = Either[ErrorX, QueueBalancing]

  def  genQueue(events:List[EventX],payload:ReplicationSchema,nodexs:Map[String,NodeX],clientId:String="")(implicit ctx:NodeContext):IO[GenerateQueue]= {
    val replicaNodesInSchema = payload.data.keys.toList
    val replicaNodesInWhere  = payload.data.values.toList.flatMap(_.where)
    val replicaNodes         = (replicaNodesInSchema ++ replicaNodesInWhere).distinct
//    Check if all replicanodes exists
    val replicaNodes0        = replicaNodes.map(n=>nodexs.get(n)).filter(_.isDefined).map(_.get)
    val _replicaNodes0       = replicaNodes.filterNot(n=> nodexs.isDefinedAt(n))

    val program = replicaNodes.traverse{ nId =>
      val rs      = payload.data(nId)
      val operations = rs.what.zipWithIndex.traverse{
        case (w,index1)=>
          for {
            _                      <- IO.unit
            maybeRf                = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption)
            forceCreate            = w.metadata.get("FORCE_NODE_CREATION").flatMap(_.toBooleanOption).getOrElse(false)
            balancing              = w.metadata.get("LOAD_BALANCE").flatMap(_.toBooleanOption).getOrElse(true)
            _replicaNodes          = rs.where ++ List(nId)
            _replicaNodeXsfiltered = _replicaNodes.map(rNId=>nodexs.get(rNId)).filter(_.isDefined).map(_.get)
            //                  .filter(_.ufs.diskUF)

            newReplicaNodes        <- maybeRf match {
              case Some(rf) =>
                for {
                  _ <- IO.unit
                  diffRf          = rf - _replicaNodes.length
                  _ <- ctx.logger.debug(s"REPLICATION_FACTOR_DIFF $diffRf")
                  res <- if(diffRf == 0 || diffRf < 0) _replicaNodes.pure[IO]
                  else {
                    val _diffRf = rf - nodexs.size

                    if(ctx.config.elasticity  && _diffRf > 0){
                      val nrs        = (0 until _diffRf).map(_=>NodeReplicationSchema.empty(id = "")).toList
                      val responseIO = nrs.traverse{n => ctx.config.systemReplication.createNode(nrs = n)}
                      for {
                        _        <- IO.unit
                        response <- responseIO
                        nodes    = response.map(_.nodeId) ++ _replicaNodes
                      } yield nodes
                    } else {

                      _replicaNodes.pure[IO]
                    }
                  }
                  //                      else IO.pure(_replicaNodes))
                } yield res
              case None => _replicaNodes.pure[IO]
            }
            res            <- newReplicaNodes.zipWithIndex.traverse{
              case(rNId,index2) =>
                val operationId = mx.cinvestav.commons.utils.generateNodeId(prefix = "op",len=10,autoId=true)
                for {
                  arrivalTime0 <- IO.monotonic.map(_.toNanos)
                  serialNumber <- ctx.state.get.map(_.lastSerialNumber+index2)
                  upi          = Upload(
                    operationId  = operationId,
                    serialNumber = serialNumber,
                    arrivalTime  = arrivalTime0,
                    objectId     = w.id,
                    objectSize   = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L) ,
                    clientId     = clientId,
                    metadata     = w.metadata,
                    nodeId       = rNId
                  )
                  _            <- ctx.logger.info(s"UPLOAD ${upi.objectId} ${rNId} 0")
                } yield upi
            }

            _   <- ctx.state.update(s=>s.copy(lastSerialNumber = s.lastSerialNumber+res.length))
          } yield res


      }.map(_.flatten).map(xs=>nId -> xs)
      operations
    }.map(_.toMap)

    def inner() ={
      val x = replicaNodes0.traverse{ n =>
        val rp             = payload.data(n.nodeId)
//      pivot and the where nodes
        val where   = rp.where ++ List(n.nodeId)
        val wAR     = where.length
//
        val AR = nodexs.filterNot{
          case (nId,n)=>
            where.contains(nId)
        }
        val what           = rp.what
        val whatTotalSize  = what.map(_.metadata.getOrElse("OBJECT_SIZE","0").toLong).sum
//        First case - Where is empty and Replication factor is not defined
        val y = what.traverse{ w=>

          for {
            _                      <- IO.unit
            maybeRf                = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption)
            objectSize             = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
            forceCreate            = w.metadata.get("FORCE_NODE_CREATION").flatMap(_.toBooleanOption).getOrElse(false)
            balancing              = w.metadata.get("LOAD_BALANCE").flatMap(_.toBooleanOption).getOrElse(true)
            elasticity             = w.metadata.get("ELASTICITY").flatMap(_.toBooleanOption).getOrElse(ctx.config.elasticity)

            newReplicaNodes        <- maybeRf match {
              case Some(rf) =>
                  val  warRFDiff = rf - wAR
                  val arRFDiff   = rf - AR.size
//  ____________________________________________________________________________________________________________________
//               RF == len(Where)
                 if(rf == wAR) where.pure[IO] <* ctx.logger.debug("FIRST")
//               RFDIFF > 0 means that there are not sufficient declared nodes in where clause.
                 else if (rf > wAR){
                   if(balancing && AR.size > warRFDiff ) {
                     val ns = NonEmptyList.fromListUnsafe(AR.values.toList)
                     val maybeSelectedNodes = UploadControllerV2.balance(events =events)(objectSize = objectSize,nodes =ns ,rf=rf)
                     maybeSelectedNodes match {
                       case Some(value) => value.map(_.nodeId).pure[IO] <* ctx.logger.debug("SECOND. 0")
                       case None =>
                         Nil.pure[IO] <* ctx.logger.debug("SECOND. 1")
                     }
                   } else if (elasticity && AR.size < warRFDiff){
                     for {
                       _           <- ctx.logger.debug(s"RF > wAR -> Create $warRFDiff nodes")
                       nodes       <- if(ctx.config.elasticityTime == "DEFERRED"){
                         val newNodesIds = (0 until warRFDiff).map(_=>utils.generateStorageNodeId(autoId=true)).toList
                         val nrs         = newNodesIds.map(id=>NodeReplicationSchema.empty(id = id))
                         val responseIO    = nrs.traverse{n => ctx.config.systemReplication.createNode(nrs = n)}
                         responseIO.start *> (where ++ newNodesIds).pure[IO]
                       }  else{
                         val nrs         = (0 until warRFDiff).toList.map(_=> NodeReplicationSchema.empty(id = ""))
                         val responseIO = nrs.traverse{n => ctx.config.systemReplication.createNode(nrs = n)}
                         for {
                           createdNs <- responseIO.map(_.map(_.nodeId))
                           nodes    = where ++ createdNs
                         } yield nodes
                       }
                     } yield nodes
                   } else where.pure[IO] <* ctx.logger.debug("")
                 }
                 else where.pure[IO] <* ctx.logger.debug("THIRD")
              case None => where.pure[IO]
            }
          } yield (w->newReplicaNodes)

        }

        for {
          selectedNodes <- y
        } yield (rp -> selectedNodes.toMap)
      }
      val xx = x.map(_.toMap)
       xx
    }

//    All replica nodes are defined
    if(_replicaNodes0.isEmpty) {
      for {
         _              <- IO.unit
         x              <-  inner().map(xs=>xs.asRight[ErrorX])
      } yield x
    } else {
      for{
        _ <- ctx.logger.debug("SOME NODE IS NOT DEFINED")
        er = NoDefinedNodes(Nil).asInstanceOf[ErrorX].asLeft[QueueBalancing]
      } yield er
    }

  }


  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{

    case req@GET -> Root / "upload" =>
      val app = for {
//    _____________________________________________________________________
      _                  <- s.acquire
      arrivalTime        <- IO.monotonic.map(_.toNanos)
      payload            <- req.as[UploadRequest]
//    __________________________________________________________________________________________________________________
      currentState       <- ctx.state.get
      nodesQueue         = currentState.nodeQueue
      avgServiceTimes    = Operations.getAVGServiceTime(operations= currentState.operations)
      nodexs             = currentState.nodes
      ar                 = nodexs.size
//    __________________________________________________________________________________________________________________
      serviceTime        <- IO.monotonic.map(_.toNanos - arrivalTime)
      response           <- Ok()
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
      queue            = currentState.nodeQueue
      nodeQueue        = queue.getOrElse(nodeId,Nil)
      completeQueue    = currentState.completedQueue.getOrElse(nodeId,Nil)
      maybeUp          = nodeQueue.find(_.operationId == operationId)
      lastCompleted    = completeQueue.maxByOption(_.arrivalTime)

      response         <- maybeUp match {
        case Some(o) =>
          o match {
            case up:Upload =>
              val wt  = lastCompleted.map(_.arrivalTime).getOrElse(0L) - up.arrivalTime
              val completed  = UploadCompleted(
                operationId  = up.operationId,
                serialNumber = up.serialNumber,
                arrivalTime  = serviceTimeStart,
                serviceTime  = serviceTimeStart - up.arrivalTime,
                waitingTime  = if(wt < 0L ) 0L else wt,
                idleTime     = if(wt < 0L) wt*(-1) else 0L,
                objectId     = objectId,
                nodeId       = nodeId,
                metadata      = Map.empty[String,String]
              )
               val saveOp = ctx.state.update{ s=>
                 s.copy(
                   completedQueue =  s.completedQueue.updatedWith(nodeId)(op=>op.map( x => x :+ completed )),
                   operations     =  s.operations :+completed
                 )
               }
              NoContent()
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
        nrss         = payload.nodes
        _            <- nrss.traverse{ nrs=>
          ctx.config.systemReplication.createNode(nrs =nrs)
        }.start
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
