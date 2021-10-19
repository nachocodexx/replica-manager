package mx.cinvestav
import breeze.linalg._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.balancer.v3.UF
import mx.cinvestav.commons.events.{Downloaded, EventXOps, Evicted, Replicated, Uploaded}
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events

import java.util.UUID
//
import mx.cinvestav.Declarations.{NodeContext, ObjectId,nodeXOrder}
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
import mx.cinvestav.commons.balancer.v3.{RoundRobin,PseudoRandom,Balancer=>BalancerV3,LeastConnections}
import mx.cinvestav.server.HttpServer.{PushResponse, ReplicationResponse}
//import mx.cinvestav.commons.t
//
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.typelevel.ci.CIString

import scala.collection.SortedSet
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.http4s._
//import org.http4s.implicits._
import org.http4s.blaze.client.BlazeClientBuilder
//
import concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps

object Helpers {
//
    def uploadSameLogic(node: NodeX,currentNodeId:String,objectId:String,objectSize:Long,arrivalTime:Long,response:Response[IO])(implicit ctx:NodeContext): IO[Unit] =
      for {
        uploadServiceTime      <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
        responseHeaders        = response.headers

        maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
        maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)

        _                 <- maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize) match {
          case Some((evictedObjectId,evictedObjectSize)) => ctx.state.update{ s=>
            val newEvent = Uploaded(
              eventId = UUID.randomUUID().toString,
              nodeId = currentNodeId,
              serialNumber = s.events.length,
              objectId = objectId,
              objectSize = objectSize,
              timestamp = arrivalTime,
              selectedNodeId = node.nodeId,
              milliSeconds = uploadServiceTime
            )
            val evictedEvent = Evicted(
              eventId = UUID.randomUUID().toString,
              serialNumber = s.events.length+1,
              nodeId = currentNodeId,
              objectId =  evictedObjectId,
              objectSize = evictedObjectSize,
              fromNodeId = node.nodeId,
              timestamp = arrivalTime+10,
              milliSeconds = 1
            )
            s.copy(events =  s.events :+ newEvent :+ evictedEvent)
          }
          case None => ctx.state.update { s =>
            val newEvent = Uploaded(
              eventId = UUID.randomUUID().toString,
              nodeId = currentNodeId,
              serialNumber = s.events.length,
              objectId = objectId,
              objectSize = objectSize,
              timestamp = arrivalTime,
              selectedNodeId = node.nodeId,
              milliSeconds = uploadServiceTime
            )
            s.copy(events = s.events :+ newEvent)
          }
        }
      } yield ()

  //  _________________________________
  def replicationDaemon(period:FiniteDuration= 10 seconds)(implicit ctx:NodeContext): IO[Unit] = {
    fs2.Stream.awakeEvery[IO](period=period) .evalMap{ elapsedDuration=>
      for {
        _              <- ctx.logger.debug("<---REPLICATION DAEMON v.6--->")
        currentState   <- ctx.state.get
        currentNodeId  = ctx.config.nodeId
        eventCount     = currentState.events.length
        events         = Events.filterEvents(EventXOps.OrderOps.byTimestamp(currentState.events))
        matrix         = Events.generateTemperatureMatrixV2(events = events)
        _              <- if(matrix.size ==0) IO.unit
        else ctx.logger.debug(matrix.toString)
      } yield ( )
    }.compile.drain
  }
  def replicateEveryXSec()(implicit ctx:NodeContext): IO[FiberIO[Unit]] = fs2.Stream.awakeEvery[IO](30 seconds)
    .evalMap{ time=>
      for {
        _              <- ctx.logger.debug("<---REPLICATION DAEMON v.6--->")
        currentState   <- ctx.state.get
        currentNodeId  = ctx.config.nodeId
        eventCount     = currentState.events.length
        events         = Events.filterEvents(EventXOps.OrderOps.byTimestamp(currentState.events))
        matrix         = Events.generateMatrix(events = events)
        lb             = currentState.uploadBalancer
        _              <- if(matrix.size == 0)  for {
          _ <- ctx.logger.debug("NO DOWNLOADS YET >.<")
          _<- ctx.logger.debug("____________________________________________")
        } yield ()
        else for{
              _                 <- IO.unit
              ars               = Events.toNodeX(events = events)
              arMap             = ars.map(node => node.nodeId -> node).toMap
              filteredArs       = ars.filter(_.availableCacheSize>0)
              _                 <- if(filteredArs.isEmpty) for {
                _ <- ctx.logger.debug("NO AR -> CREATE ONE and replicate")
              } yield ()
              else for {
                _                  <- ctx.logger.debug(s"FILTERED_ARS $filteredArs")
                colSumVector       = sum(matrix(::, *)).t
                totalDownloads     = sum(colSumVector)
                heatVector         = (colSumVector/totalDownloads).toScalaVector()
//
                threshold          = 0.95
//
                filterHeatMap      = heatVector.zipWithIndex.filter{
                  case (d, i) => d > threshold
                }
                objectIds          = Events.getObjectIds(events = events)
                distributionSchema = Events.generateDistributionSchema(events=events)
                replicatedObjects  = filterHeatMap.map(_._2).map(objectIds).toList
//
                _                  <- ctx.logger.debug(objectIds.toString())
                _                  <- ctx.logger.debug(heatVector.toString())
                _                  <- ctx.logger.debug(s"REPLICATE OBJECTS: $replicatedObjects")
                //                  __________________________________________________
                availableNodesToReplicate = distributionSchema.map{
                  case (objectId, replicas) =>
                    objectId -> filteredArs.map(_.nodeId).toSet.diff(replicas.toSet).toList
                }.map{
                  case (objectId, nodeIds) =>
                    objectId -> nodeIds.map(arMap)
                }.filter(_._2.nonEmpty).map{
                  case (str, value) =>  str-> NonEmptyList.fromListUnsafe(value)
                }
//              ______________________________________________________________________

                (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
//               ____________________________________________________________--
                _  <- availableNodesToReplicate.toList.traverse{
                  case (objectId, nodeCandidates) =>
                    val maybeObjectSize   = events.filter{
                      case uploaded:Uploaded => true
                      case _=> false
                    }.map(_.asInstanceOf[Uploaded]).filter(_.objectId == objectId).map(_.objectSize).headOption
                    //
                    val maybeSelectedNode = maybeObjectSize.mproduct(_=>lb)
                      .flatMap{
                        case (size,lbb) =>
                          lbb.balance(objectSize=size,nodes=nodeCandidates).map(n=>(n,size))
                      }

                    for {
//                      _ <- ctx.logger.debug(s"-> SEND REPLICA TO $maybeSelectedNode")

                      _ <- maybeSelectedNode match {
                        case Some((selectedNode,replicateObjectSize)) =>  for {
                          _        <- ctx.logger.debug(s"REPLICATE $objectId to ${selectedNode.nodeId}")
                          now       <- IO.realTime.map(_.toMillis)
                          pullNodeId = distributionSchema(objectId).head
                          pullFrom = arMap(pullNodeId).httpUrl+s"/api/v6/download/$objectId"
                          unsafeUri      = s"${selectedNode.httpUrl+"/pull"}"
                          _        <- ctx.logger.debug(s"URI $unsafeUri")
                          _        <- ctx.logger.debug(s"PULL_FROM $pullFrom")
                          req      = Request[IO](
                            method = Method.POST,
                            uri = Uri.unsafeFromString(unsafeUri),
                            headers = Headers(
                              Header.Raw(CIString("Pull-From"),pullFrom)
                            )
                          )

                          pullResponse <- client.toHttpApp.run(req)
                          pullHeaders  = pullResponse.headers
                          _ <- ctx.logger.debug("BEFORE STs")
//
                          downloadSt   = pullHeaders.get(CIString("Download-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                          uploadSt     = pullHeaders.get(CIString("Upload-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                          pullSt       = pullHeaders.get(CIString("Pull-Service-Time")).map(_.head.value).flatMap(_.toLongOption).get
                          _ <- ctx.logger.debug(s"DOWNLOAD_ST $downloadSt")
                          _ <- ctx.logger.debug(s"UPLOAD_ST $uploadSt")
                          _ <- ctx.logger.debug(s"PULL_ST $pullSt")

                          serviceTime <- IO.realTime.map(_.toMillis).map(_ - now)
                          newEvents = List(
                            Replicated(
                              eventId = UUID.randomUUID().toString,
                              serialNumber = eventCount,
                              nodeId = currentNodeId,
                              objectId = objectId,
                              objectSize =replicateObjectSize,
                              replicaNodeId = pullNodeId,
                              timestamp = now,
                              milliSeconds = serviceTime
                            ),
                            Downloaded(
                              eventId = UUID.randomUUID().toString,
                              serialNumber = eventCount+1,
                              nodeId = currentNodeId,
                              objectId = objectId,
                              objectSize = replicateObjectSize,
                              selectedNodeId = pullNodeId,
                              timestamp = now+10,
                              milliSeconds = downloadSt
                            ),
                            Uploaded(
                              eventId = UUID.randomUUID().toString,
                              serialNumber = eventCount+2,
                              nodeId = currentNodeId,
                              objectId = objectId,
                              objectSize = replicateObjectSize,
                              selectedNodeId = selectedNode.nodeId,
                              timestamp = now+100,
                              milliSeconds = uploadSt
                            ),
                          )
                          _ <- ctx.state.update(s=>s.copy(events = s.events ++ newEvents))
                          _ <- ctx.logger.debug(s"STATUS $pullResponse")
                        } yield ()
                        case None => ctx.logger.debug(s"NO AVAILABLE RESOURCES TO REPLICATE $objectId")
                      }
                      _ <- ctx.logger.debug("____________________________________________")
                    } yield ()
                }
//              ___________________________________________
                _ <- finalizer
              } yield ()

            } yield ()
          } yield ()
        }
        .compile.drain.start
//    } yield ()
  //  _____________________________________

  // Init new download load balancer
  def initDownloadLoadBalancer(loadBalancerType:String="LC",nodes:NonEmptyList[NodeX])(implicit ctx:NodeContext): IO[Balancer[NodeX]] = for {
    _      <- IO.unit
    newLb  = LoadBalancer[NodeX](loadBalancerType,xs= nodes)
    _      <- ctx.state.update(s=>s.copy(downloadLB =  newLb.some))
  } yield newLb
  def initDownloadLoadBalancerV3()(implicit ctx:NodeContext): IO[BalancerV3] = for {
    _      <- IO.unit
    newLb  = LeastConnections()
    _      <- ctx.state.update(s=>s.copy(
      downloadBalancer =  newLb.some
    ))
  } yield newLb
// Init new load balancer

  def initLoadBalancerV3()(implicit ctx:NodeContext): IO[BalancerV3] = for {
    _       <- IO.unit
    newLbV3 = UF()
    _      <- ctx.state.update(s=>
      s.copy(
        uploadBalancer = newLbV3.some
      )
    )
  } yield newLbV3
  def initLoadBalancer(loadBalancerType:String="LC",
                       nodes:NonEmptyList[NodeX])(implicit ctx:NodeContext): IO[Balancer[NodeX]] = for {
    _       <- IO.unit
    newLb   = LoadBalancer[NodeX](loadBalancerType,xs= nodes)
//    newLbV3 = PseudoRandom()
    _      <- ctx.state.update(s=>
      s.copy(
        lb =  newLb.some,
//        uploadBalancer = newLbV3.some
      )
    )
  } yield newLb

//  Update node metadata -> update schema
  def commonLogic(arrivalTime:Long,req:Request[IO])(selectedNode:NodeX)(implicit ctx:NodeContext): IO[Response[IO]] ={
      for {
        _              <- IO.unit
//        headers        = req.headers
        selectedNodeId = selectedNode.nodeId
        _              <- ctx.logger.debug(s"CACHE_NODE_REQ $req")
        response       <-Helpers.redirectTo(selectedNode.httpUrl,req)
        payload        <- response.as[PushResponse]
//        payloadGUID    = ObjectId(payload.guid)
        //
//        responseHeaders  = response.headers
//        maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
//        maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)

        //          UPDATE CAPACITY OF A CACHE NODE
        //         Update schema based on eviction
//        _ <- maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize) match {
//          case Some( ( evictedObjectId,evictedObjectSize)) =>  for {
//            _              <- ctx.logger.debug(s"EVICTION $evictedObjectId of size $evictedObjectSize")
//            _              <- ctx.state.update{ s=>
//              val evicted = Evicted(
//                eventId = UUID.randomUUID().toString,
//                serialNumber = s.events.length,
//                nodeId = ctx.config.nodeId,
//                objectId = evictedObjectId,
//                objectSize = evictedObjectSize,
//                fromNodeId = selectedNodeId,
//                timestamp = arrivalTime,
//                milliSeconds = 1
//              )
//              s.copy(events =  s.events :+ evicted)
//            }
//          } yield ()
//          case None => ctx.logger.debug("NO EVIcTION!")
//        }

        endAt         <- IO.realTime.map(_.toMillis)
        time          = endAt - arrivalTime
        _             <- ctx.logger.info(s"UPLOAD ${payload.guid} ${payload.objectSize} ${selectedNode.nodeId} $time")
      } yield response
    }

  def placeReplicaInAR(arrivalTime:Long,
                       arMap:Map[String,NodeX],
                       lb:Balancer[NodeX],
                       req:Request[IO]
                      )
                      (objectId: ObjectId,
                       availableResources:List[String])
                      (implicit ctx:NodeContext): IO[Response[IO]] =
    for {
      _                           <- ctx.logger.debug(s"THERE ARE FREE SPACES $availableResources")
      headers                     = req.headers
      maybeGuid                   = headers.get(CIString("guid")).map(_.head.value)
      subsetNodes                 = NonEmptyList.fromListUnsafe(availableResources.map(x=>arMap(x)))
      rf                          = 1
      (newBalancer,selectedNodes) = Balancer.balanceOverSubset(balancer = lb, rounds= rf ,subsetNodes =subsetNodes)
      _                           <- ctx.logger.debug(s"SELECTED_NODES $selectedNodes")
      _                           <- ctx.state.update(s=>s.copy(lb = newBalancer.some))
      fiber                       <- selectedNodes.traverse(Helpers.commonLogic(arrivalTime=arrivalTime,req=req)(_))
      endAt                       <- IO.realTime.map(_.toMillis)
      milliSeconds                = endAt - arrivalTime
      payloadRss = ReplicationResponse(
        guid = objectId.value,
        replicas= selectedNodes.toList.map(_.nodeId),
        milliSeconds =  milliSeconds ,
        timestamp = arrivalTime
      )
      _res  <- Ok(payloadRss.asJson)
    } yield _res

  //  Create a SN and update the schema

    def redirectToWithClient(client:Client[IO])(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
      newReq = req.withUri(
        Uri.unsafeFromString(nodeUrl)
          .withPath(req.uri.path)
      )
      _ <- IO.println(s"NEW_REQ $newReq")
      response <- client.toHttpApp.run(newReq)
    } yield response

    def redirectTo(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
  //    newReq = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
      (client, finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
      response            <- redirectToWithClient(client)(nodeUrl=nodeUrl,req=req)
  //    response <- client.toHttpApp.run(newReq)
      _ <- finalizer
    } yield response
}

