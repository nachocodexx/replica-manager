package mx.cinvestav.server.controllers

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.{Download, NodeContext, Upload}
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import mx.cinvestav.commons.types.{NodeReplicationSchema, NodeX, ReplicationSchema}
import org.http4s.circe.CirceEntityDecoder._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Declarations
import mx.cinvestav.events.Events
import mx.cinvestav.commons.events.{EventX, EventXOps}
import org.typelevel.ci.CIString


object UploadV3 {

  def  genQueue(events:List[EventX],payload:ReplicationSchema,nodexs:Map[String,NodeX],clientId:String="")(implicit ctx:NodeContext)= {
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
            //                _                      <- ctx.logger.debug(s"REPLICATION_FACTOR $maybeRf")
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
                val operationId = mx.cinvestav.commons.utils.generateNodeId(prefix = "op",len=10,autoId=true,suffix="")
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

    def inner(availableNodes:Map[String,NodeX] = Map.empty[String,NodeX]):Map[String,NodeX] ={
      val x = replicaNodes0.map{ n =>
        val rp            = payload.data(n.nodeId)
        val what          = rp.what
        val whatTotalSize = what.map(_.metadata.getOrElse("OBJECT_SIZE","0").toLong).sum
//        First case - Where is empty and Replication factor is not defined

        val y = what.traverse{ w=>

          for {
            _                      <- IO.unit
            maybeRf                = w.metadata.get("REPLICATION_FACTOR").flatMap(_.toIntOption)
            objectSize             = w.metadata.get("OBJECT_SIZE").flatMap(_.toLongOption).getOrElse(0L)
            forceCreate            = w.metadata.get("FORCE_NODE_CREATION").flatMap(_.toBooleanOption).getOrElse(false)
            balancing              = w.metadata.get("LOAD_BALANCE").flatMap(_.toBooleanOption).getOrElse(true)
            elasticity             = w.metadata.get("ELASTICITY").flatMap(_.toBooleanOption).getOrElse(ctx.config.elasticity)
            _replicaNodes          = rp.where ++ List(n.nodeId)
//            _replicaNodeXs         = _replicaNodes.map(nId=>nodexs(nId) )

            newReplicaNodes        <- maybeRf match {
              case Some(rf) =>
                  val  warRFDiff    = rf - _replicaNodes.length
                  val arRFDiff = rf - nodexs.size
//  ____________________________________________________________________________________________________________________
                 if(rf == _replicaNodes.length) _replicaNodes.pure[IO]
//               RFDIFF > 0 means that there are not sufficient declared nodes in where definition.
                 else if (warRFDiff > 0){
                   for {
                     _ <- IO.unit
//                   BALANCE: ACTIVE , ELASTICITY: FALSE , AR > RF
                     selected  <- if(balancing && !elasticity && arRFDiff <0) for {
                       _       <- ctx.logger.debug(s"BALANCING ^ !ELASTICITY ^ REAL_RF_DIFF $arRFDiff")
                       war     = _replicaNodes.length
                       _nodes  = if(war == rf) _replicaNodes
                       else if (war < rf) {
                         val _ar =
                           UploadControllerV2.balance(events = events)(objectSize = objectSize ,rf = warRFDiff,nodes = _ar).getOrElse(Nil)
                       }
                       else _replicaNodes
//                       nodes   = UploadControllerV2.balance(events = events )(objectSize = objectSize,nodes =  _nodes,rf=warRFDiff)
                       nodes = Nil
                     } yield nodes
//                   BALANCE: ACTIVE , ELASTICITY: FALSE , AR < RF
                     else if(balancing && !elasticity && arRFDiff > 0){
                       for {
                         _     <- ctx.logger.debug(s"BALANCING ^ !ELASTICITY ^ REAL_RF_DIFF $arRFDiff")
                         nodes = Nil
                       } yield nodes
                     }
                     else {
                       IO.pure(Nil)
                     }
                   } yield ()
                 }
                 else {
                   for {
                     _               <- IO.unit
                     diffRf          = rf - _replicaNodes.length
                     _               <- ctx.logger.debug(s"REPLICATION_FACTOR_DIFF $diffRf")
                     res             <- if(diffRf == 0 || diffRf < 0) _replicaNodes.pure[IO]
                     else {
                       val _diffRf = rf - nodexs.size
                       //                    if(balancing && )
                       if(ctx.config.elasticity && _diffRf >0 ){
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
                 }
              case None => _replicaNodes.pure[IO]
            }
          } yield ()

        }
      }
        Map.empty[String,NodeX]

    }

//    All replica nodes defined in rs exists
    if(_replicaNodes0.isEmpty) {
      for {
         _              <- IO.unit
         availableNodes = replicaNodes0.map(n=>n.nodeId -> n).toMap
         x              =  inner( availableNodes = availableNodes )
      } yield ()
    } else {
      for{
        _ <- IO.unit
        _ <- if(ctx.config.elasticity) {
          IO.unit
        } else {
          IO.unit
        }
      } yield ()
    }

  }


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "upload" /"completed" => for{
      serviceTimeStart <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      headers          = req.headers
      nodeId           = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("NODE_ID")
      objectIds        = headers.get(CIString("Object-Id")).map(_.map(_.value).toList).getOrElse(Nil).distinct
      queue            = currentState.nodeQueue
      nodeQueue        = queue.getOrElse(nodeId,Nil)
      newNodeQueue     = nodeQueue.filterNot (o => nodeQueue.contains(o.objectId) )
      completedOps     <- nodeQueue.filter(o => nodeQueue.contains(o.objectId) ).traverse {
        case d: Download => IO.unit
        case o@(u: Upload) =>
          val serviceTime  = serviceTimeStart - u.arrivalTime
          ctx.logger.info(s"UPLOAD_COMPLETED ${o.objectId} ${o.nodeId} $serviceTime")
      }

      response     <- NoContent()
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
        x <- genQueue(events = events, payload = payload,nodexs=nodexs)
//        queueX               <- ???
//        _            <- ctx.state.update{s=>s.copy(nodeQueue = s.nodeQueue |+| queueX)}
        response     <- Ok()
      } yield response
  }

}
