package mx.cinvestav.server.controllers

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.effects.Log
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{EventXOps, Evicted, Get, Missed, Put}
import mx.cinvestav.commons.balancer.v3.{PseudoRandom, RoundRobin}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.events.Events
import mx.cinvestav.events.Events.GetInProgress
import org.http4s.{AuthedRoutes, Header, Headers, Response}
import org.http4s.dsl.io._
import org.typelevel.ci.CIString
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors, retryingOnFailures}

import java.util.{Random, UUID}
//import
//
import concurrent.duration._
import language.postfixOps

object DownloadController {

  def apply()(implicit ctx:NodeContext) = {

    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "download" / UUIDVar(guid)   as user => for {
        arrivalTime      <- IO.realTime.map(_.toMillis)
        arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
        currentState     <- ctx.state.get
//        _                <- currentState.s.acquire
        rawEvents        = currentState.events
        events           = Events.orderAndFilterEventsMonotonic(rawEvents)
        currentNodeId    = ctx.config.nodeId
        schema           = Events.generateDistributionSchema(events = events)
        arMap            = Events.getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
        maybeLocations   = schema.get(guid.toString)
//        maybeLB          = currentState.downloadBalancer.getOrElse(PseudoRandom())
        req              = authReq.req
//        headers
        operationId      = req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectSize       = req.headers.get(CIString("Object-Size")).map(_.head.value).flatMap(_.toLongOption).getOrElse(0L)
        response         <- maybeLocations match {
          case (Some(locations))=> for {
            _                     <- IO.unit
            subsetNodes          = locations.traverse(arMap.get).get.toNel.get
            maybeSelectedNode    = if(subsetNodes.length ==1) subsetNodes.head.some
            else Events.balanceByReplica(currentState.downloadBalancerToken,objectSize = objectSize)(
                          guid = guid.toString,
                          arMap = subsetNodes.map(x=>x.nodeId->x).toList.toMap,
                          events=events)
            _                    <- ctx.logger.debug(s"SELECTED_NODE ${maybeSelectedNode.map(_.nodeId)}")
            response             <- maybeSelectedNode match {
              case Some(selectedNode) => for {
                _                   <- IO.unit
                selectedNodeId       = selectedNode.nodeId
                serviceTimeNanos0    <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
                //
//                response             <- Helpers.redirectTo(selectedNode.httpUrl,req)
                retryPolicy         = RetryPolicies.limitRetries[IO](100) join RetryPolicies.exponentialBackoff[IO](1 seconds )
                response             <- retryingOnAllErrors[Response[IO]](
                  policy = retryPolicy,
                  onError = (e:Throwable,d:RetryDetails)=> ctx.errorLogger.error(e.getMessage+s" $guid")
                )(Helpers.redirectTo(selectedNode.httpUrl,req))
//
                endAt                <- IO.realTime.map(_.toMillis)
                endAtNanos           <- IO.monotonic.map(_.toNanos)
                serviceTimeNanos     = endAtNanos - arrivalTimeNanos
//
                rawBytes             <- response.body.compile.to(Array)
                // WARNING_ZONE
                guidH                 = response.headers.get(CIString("Object-Id")).map(_.head).get
                objectSizeH           = response.headers.get(CIString("Object-Size")).map(_.head).get
                objectSizeV           = objectSizeH.value.toLongOption.getOrElse(0L)
                levelH                = response.headers.get(CIString("Level")).map(_.head).get
                levelV                = levelH.value
                objectNodeIdH         = response.headers.get(CIString("Node-Id")).map(_.head).get
                newHeaders            = Headers(
                  guidH,
                  objectSizeH,
                  levelH,
                  objectNodeIdH,
                  Header.Raw(CIString("Response-Time"),serviceTimeNanos0.toString)
                )
                //            ___________________________________________________________________________________
                _                     <- ctx.logger.info(s"DOWNLOAD $selectedNodeId $guid $serviceTimeNanos0 $levelV $operationId")
                _                     <- Events.saveEvents(
                  events = List(
                    Get(
                      serialNumber=  0,
                      objectId = guid.toString,
                      objectSize= objectSizeV,
                      timestamp = arrivalTime,
                      nodeId = selectedNode.nodeId,
                      serviceTimeNanos = serviceTimeNanos,
                      userId = user.id.toString,
                      correlationId = operationId
                    )
                  )
                )
                _                     <- ctx.logger.info(s"DOWNLOAD_COMPLETED $selectedNodeId $guid $serviceTimeNanos $levelV $operationId")
                newResponse <- Ok(
                  fs2.Stream.emits(rawBytes).covary[IO],
                  newHeaders
                )
              } yield newResponse
              case None => InternalServerError()
            }
          } yield response
          case None => for {
            _                       <- ctx.logger.debug(s"MISS $guid")
            nodes                   = Events.getAllNodeXs(events = events)
            nodesWithAvailablePages = nodes.filter(_.availableCacheSize>0)
            arMap                   = nodes.map(x=>x.nodeId->x).toMap
            maybeSelectedNode            <- if(nodes.length==1) nodes.headOption.pure[IO]
            else if(nodesWithAvailablePages.isEmpty)  for{
              _                  <- ctx.logger.debug("ALL NODES ARE FULL - SELECT A NODE RANDOMLY")
              maybeSelectedNode  = Events.balanceByReplica(downloadBalancer = "PSEUDO_RANDOM")(guid= guid.toString,arMap = arMap,events=events)
//              _                 <- if( (nodes.length < currentState.maxAR) && ctx.config.serviceReplicationDaemon)
                _ <- if(currentState.serviceReplicationDaemon) Helpers.createNode().start.void else IO.unit
//              else IO.unit
            } yield maybeSelectedNode
            else nodesWithAvailablePages.maxByOption(_.availableCacheSize).pure[IO]
//
            response  <- maybeSelectedNode match {
              case Some(selectedNode) => for {
                _ <- ctx.logger.debug(s"SELECTED_NODE ${selectedNode.nodeId}")
                selectedNodeId   = selectedNode.nodeId
//                response         <- retryingOnFailures[Response[IO]](
//                  wasSuccessful = (a:Response[IO])=> IO.pure(a.status.code == 200) ,
//                  onFailure =  (a:Response[IO],d:RetryDetails) => for {
//                    serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
//                    _                <- ctx.logger.info(s"DOWNLOAD_RETRY $selectedNodeId $guid $serviceTimeNanos LOCAL $operationId")
//                  } yield () ,
//                  policy =  RetryPolicies.limitRetries[IO](maxRetries = ctx.config.downloadMaxRetry) join RetryPolicies.exponentialBackoff[IO]( ctx.config.downloadBaseDelayMs milliseconds)
//                )(Helpers.redirectTo(selectedNode.httpUrl,req))
                response         <- Helpers.redirectTo(selectedNode.httpUrl,req)
//                newResponse <- Ok()
                responseStatus   = response.status
                newResponse      <- if(responseStatus.code == 200)
                  {
                    for {

                      serviceTimeNanos <- IO.monotonic.map(_.toNanos).map( _ - arrivalTimeNanos)
                      responseHeaders  = response.headers
                      //
                      guidH            = responseHeaders.get(CIString("Object-Id")).map(_.head).get
                      objectSizeH      = responseHeaders.get(CIString("Object-Size")).map(_.head).get
                      objectSizeV      = objectSizeH.value.toLongOption.getOrElse(0L)
                      levelH           = responseHeaders.get(CIString("Level")).map(_.head).get
                      levelV           = levelH.value
                      objectNodeIdH    = responseHeaders.get(CIString("Node-Id")).map(_.head).get
                      newHeaders       = Headers(guidH,objectSizeH,levelH,objectNodeIdH,
                        Header.Raw(CIString("Response-Time"),serviceTimeNanos.toString)
                      )
                      //
                      maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
                      maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)
                      _                      <- ctx.logger.debug(s"EVICTED: $maybeEvictedObjectId")
                      endAtNanos               <- IO.monotonic.map(_.toNanos)
                      downloadServiceTimeNanos = endAtNanos - arrivalTimeNanos
                      evictedMproduct = maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize)
                      _                 <- evictedMproduct  match {
                        case Some((evictedObjectId,evictedObjectSize)) => Events.saveEvents(
                          events = List(
                            Missed(
                              eventId = UUID.randomUUID().toString,
                              serialNumber = 0,
                              nodeId = selectedNodeId,
                              objectId = guid.toString,
                              objectSize = objectSizeV,
                              timestamp = arrivalTime,
                              serviceTimeNanos= 1,
                              correlationId = operationId
                            ),
                            Evicted(
                              serialNumber = 0,
                              nodeId = currentNodeId,
                              objectId =  evictedObjectId,
                              objectSize = evictedObjectSize,
                              fromNodeId = selectedNodeId,
                              timestamp = arrivalTime+1,
                              serviceTimeNanos = 1,
                              correlationId = operationId
                            ),
                            Put(
                              serialNumber = 0,
                              objectId = guid.toString,
                              objectSize = objectSizeV,
                              timestamp = arrivalTime+2,
                              nodeId = selectedNodeId,
                              serviceTimeNanos = serviceTimeNanos,
                              userId = user.id.toString,
                              correlationId = operationId
                            ),
                            Get(
                              serialNumber=  0,
                              objectId = guid.toString,
                              objectSize= objectSizeV,
                              timestamp = arrivalTime+2,
                              nodeId = selectedNodeId,
                              serviceTimeNanos = downloadServiceTimeNanos,
                              userId = user.id.toString,
                              correlationId = operationId
                            )
                          )
                        )
                        case None => Events.saveEvents(
                          events = List(
                            Missed(
                              eventId = UUID.randomUUID().toString,
                              serialNumber = 0,
                              nodeId = selectedNodeId,
                              objectId = guid.toString,
                              objectSize = objectSizeV,
                              timestamp = arrivalTime,
                              serviceTimeNanos = 1,
                              correlationId = operationId
                            ),
                            Put(
                              serialNumber = 0 ,
                              objectId = guid.toString,
                              objectSize = objectSizeV,
                              timestamp = arrivalTime+1,
                              nodeId = selectedNodeId,
                              serviceTimeNanos = serviceTimeNanos,
                              userId = user.id.toString,
                              correlationId = operationId
                            ),
                            Get(
                              serialNumber=  0,
                              objectId = guid.toString,
                              objectSize= objectSizeV,
                              timestamp = arrivalTime+2,
                              nodeId = selectedNodeId,
                              serviceTimeNanos = downloadServiceTimeNanos,
                              userId = user.id.toString,
                              correlationId = operationId
                            )
                          )
                        )
                      }
                      rawBytes         <- response.body.compile.to(Array)
                      _                <- ctx.logger.info(s"DOWNLOAD $selectedNodeId $guid $downloadServiceTimeNanos $levelV $operationId")
                      newResponse      <- Ok(fs2.Stream.emits(rawBytes).covary[IO], newHeaders)
                    } yield newResponse
                  }
                else NotFound()
              } yield newResponse
              case None => Forbidden()
            }
          } yield response
          //
//          case ((None,None ))=> for {
//            _            <- ctx.logger.debug(s"Object[$guid] NOT found and lb NOT found")
//            newResponse <- NotFound()
//          } yield newResponse
        }
//        _                <- currentState.s.release
        _ <- ctx.logger.debug("____________________________________________________")

      } yield response
    }

  }

}
