package mx.cinvestav.server

import breeze.linalg._
import cats.data.{Kleisli, NonEmptyList, OptionT}
import cats.implicits._
import cats.effect.IO
import mx.cinvestav.commons.events.{Downloaded, EventXOps, Evicted, Get, Put, Uploaded,Missed}
import mx.cinvestav.controllers.AddNode
import mx.cinvestav.events.Events
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.Helpers
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
//
import mx.cinvestav.Declarations.{NodeContext, User,nodeXOrder,eventXEncoder}
//import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.Declarations.nodeXOrder
//
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.server.AuthMiddleware
import org.http4s.{headers=>HEADERS}
import org.typelevel.ci._
//
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//

object HttpServer {
//  ____________________________________________
  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           milliSeconds:Long,
                           timestamp:Long,
                           level:Int
                         )
  case class ReplicationResponse(guid:String,replicas:List[String],milliSeconds:Long,timestamp:Long,rf:Int=1)
//  ________________________
  def authUser()(implicit ctx:NodeContext):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(IO.unit)
      headers    = req.headers
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
//      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = UUID.fromString(userId),bucketName=bucketName  ).pure[IO])
          //          _  <- OptionT.liftF(ctx.logger.debug("AUTHORIZED"))
        } yield x
        case (Some(_),None) => OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.none[IO,User]
        case (None,None )   => OptionT.none[IO,User]
      }

    } yield ress
    }

  def authMiddleware(implicit ctx:NodeContext):AuthMiddleware[IO,User] =
    AuthMiddleware(authUser=authUser)

  def authRoutes()(implicit ctx:NodeContext):AuthedRoutes[User,IO] = AuthedRoutes.of[User,IO] {
    case authReq@GET -> Root / "download" / UUIDVar(guid)   as user => for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      events        = Events.filterEvents(currentState.events)
//
      currentNodeId = ctx.config.nodeId
      schema        = Events.generateDistributionSchema(events = events)
//        currentState.schema
      arMap         = Events.toNodeX(events = events).map(x=>x.nodeId->x).toMap
      maybeLocations   = schema.get(guid.toString)
      maybeLB       = currentState.downloadBalancer
      req           = authReq.req

      response      <- (maybeLocations,maybeLB) match {
//
        case ((Some(locations),Some(lb)))=> for {
//          _                     <- ctx.logger.debug(s"Object[$guid] found and lb found")
          _                    <- IO.unit
          subsetNodes          = locations.traverse(arMap.get).get.toNel.get
          maybeSelectedNode     = lb.balance(objectSize = 0L,nodes = subsetNodes)
//          _                    <- ctx.logger.debug(s"SELECTED_NODE $maybeSelectedNode")
          response             <- maybeSelectedNode match {
            case Some(selectedNode) => for {
              _ <- IO.unit
              selectedNodeId       = selectedNode.nodeId
              serviceTime0         <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
              _                    <- ctx.logger.info(s"DOWNLOAD $selectedNodeId $guid $serviceTime0")
              //
              response             <- Helpers.redirectTo(selectedNode.httpUrl,req)
//              _                    <- ctx.logger.debug(s"DOWNLOAD_RESPONSE $response")
              endAt                <- IO.realTime.map(_.toMillis)
              serviceTime          = endAt- arrivalTime
              rawBytes             <- response.body.compile.to(Array)
              //              _ <- ctx.logger.debug("BEGORE_WARINIG_ZONE")
              // WARNING_ZONE
              guidH                 = response.headers.get(CIString("Object-Id")).map(_.head).get
              objectSizeH           = response.headers.get(CIString("Object-Size")).map(_.head).get
              objectSizeV           = objectSizeH.value.toLongOption.getOrElse(0L)
              levelH                = response.headers.get(CIString("Level")).map(_.head).get
              objectNodeIdH         = response.headers.get(CIString("Node-Id")).map(_.head).get
              newHeaders            = Headers(guidH,objectSizeH,levelH,objectNodeIdH)
//            ___________________________________________________________________________________
              objectNodeIdV         = objectNodeIdH.value
              _                     <- ctx.state.update{ s=>
                s.copy(events = s.events :+ Downloaded(
                  eventId = UUID.randomUUID().toString,
                  nodeId= currentNodeId,
                  serialNumber=  s.events.length,
                  objectId = guid.toString,
                  objectSize= objectSizeV,
                  timestamp = arrivalTime,
                  selectedNodeId = selectedNode.nodeId,
                  milliSeconds = serviceTime
                ))
              }
              _                    <- ctx.logger.info(s"DOWNLOAD_COMPLETED $selectedNodeId $guid $serviceTime")

              newResponse <- Ok(
                fs2.Stream.emits(rawBytes).covary[IO],
                newHeaders
              )
            } yield newResponse
            case None => InternalServerError()
          }
//          (newLB,selectedNode) =  Balancer.balanceOverSubset(lb,rounds=1,subsetNodes = subsetNodes)
//        } yield response
      } yield response
              //
        case (Some(locations),None )=> for {
          _             <- ctx.logger.debug(s"Object[$guid] found and lb NOT found")
          nodes         = locations.traverse(arMap.get).get.toNel.get
          newLb         <- Helpers.initDownloadLoadBalancerV3()
          maybeSelectedNode    =  newLb.balance(objectSize = 0L,nodes=nodes)
          response             <- maybeSelectedNode match {
            case Some(selectedNode) => for {
//            Select node service time
              serviceTime0    <- IO.realTime.map(_.toMillis).map(_-arrivalTime)
              selectedNodeId  = selectedNode.nodeId
              _               <- ctx.logger.info(s"DOWNLOAD $selectedNodeId $guid $serviceTime0")
              //        COMPLETED THE DOWNLOAD
              response        <- Helpers.redirectTo(selectedNode.httpUrl,req)
              //        WARINING AREA - the headers can be empty, for now it's ok.
              guidH           = response.headers.get(CIString("Object-Id")).map(_.head).get
              objectSizeH     = response.headers.get(CIString("Object-Size")).map(_.head).get
              objectSizeV     = objectSizeH.value.toLongOption.getOrElse(0L)
              levelH          = response.headers.get(CIString("Level")).map(_.head).get
              objectNodeIdH   = response.headers.get(CIString("Node-Id")).map(_.head).get
              objectNodeIdV   = objectNodeIdH.value
              newHeaders      = Headers(guidH,objectSizeH,levelH,objectNodeIdH)
//            _________________________________________________________________________________-
              endAt           <- IO.realTime.map(_.toMillis)
              serviceTime     = endAt- arrivalTime
              rawBytes        <- response.body.compile.to(Array)
              _               <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
              _               <- ctx.logger.info(s"DOWNLOAD_COMPLETED $selectedNodeId $guid $serviceTime")
              //            ADD_EVENT
              _                  <- ctx.state.update{ s=>
                s.copy(events = s.events :+ Downloaded(
                  eventId = UUID.randomUUID().toString,
                  nodeId= currentNodeId,
                  serialNumber=  s.events.length,
                  objectId = guid.toString,
                  objectSize= objectSizeV,
                  timestamp = arrivalTime,
                  selectedNodeId = selectedNode.nodeId,
                  milliSeconds = serviceTime
                ))
              }
              //
              newResponse <- Ok(fs2.Stream.emits(rawBytes).covary[IO],newHeaders)
            } yield newResponse
            case None => InternalServerError()
          }
      } yield response
  //
        case (None,Some(lb) ) => for {
//          _             <- ctx.logger.debug(s"Object[$guid] NOT found and lb found")
          _             <- IO.unit
          selectedNode  = Events.toNodeX(events = events).maxBy(_.availableCacheSize)
          selectedNodeId = selectedNode.nodeId
          _             <- ctx.logger.debug(s"SELECTED_NODE $selectedNode")
          response      <- Helpers.redirectTo(selectedNode.httpUrl,req)
          serviceTime   <- IO.realTime.map(_.toMillis).map( _ - arrivalTime)
          responseHeaders   = response.headers
          _             <-  ctx.logger.debug(s"SELECTED_NODE_RESPONSE $response")
          //
          guidH         = responseHeaders.get(CIString("Object-Id")).map(_.head).get
          objectSizeH   = responseHeaders.get(CIString("Object-Size")).map(_.head).get
          objectSizeV     = objectSizeH.value.toLongOption.getOrElse(0L)
          levelH        = responseHeaders.get(CIString("Level")).map(_.head).get
          objectNodeIdH = responseHeaders.get(CIString("Node-Id")).map(_.head).get
          newHeaders    = Headers(guidH,objectSizeH,levelH,objectNodeIdH)
          //
          maybeEvictedObjectId   = responseHeaders.get(CIString("Evicted-Object-Id")).map(_.head.value)
          maybeEvictedObjectSize = responseHeaders.get(CIString("Evicted-Object-Size")).map(_.head.value).flatMap(_.toLongOption)

          _                 <- maybeEvictedObjectId.mproduct(_=>maybeEvictedObjectSize) match {
            case Some((evictedObjectId,evictedObjectSize)) => ctx.state.update{ s=>
              val missedEvent   = Missed(
                eventId = UUID.randomUUID().toString,
                serialNumber = events.length,
                nodeId = selectedNode.nodeId,
                objectId = guid.toString,
                objectSize = objectSizeV,
                timestamp = arrivalTime,
                milliSeconds = 1
              )
              val newEvent = Uploaded(
                eventId = UUID.randomUUID().toString,
                nodeId = currentNodeId,
                serialNumber = s.events.length+1,
                objectId = guid.toString,
                objectSize = objectSizeV,
                timestamp = arrivalTime+1,
                selectedNodeId = selectedNode.nodeId,
                milliSeconds = serviceTime
              )
              val evictedEvent = Evicted(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length+2,
                nodeId = currentNodeId,
                objectId =  evictedObjectId,
                objectSize = evictedObjectSize,
                fromNodeId = selectedNode.nodeId,
                timestamp = arrivalTime+2,
                milliSeconds = 1
              )
              s.copy(events =  s.events :+  missedEvent:+ newEvent :+ evictedEvent)
            }
            case None => ctx.state.update { s =>

              val missedEvent   = Missed(
                eventId = UUID.randomUUID().toString,
                serialNumber = events.length,
                nodeId = currentNodeId,
                objectId = guid.toString,
                objectSize = objectSizeV,
                timestamp = arrivalTime,
                milliSeconds = 1
              )
              val newEvent = Uploaded(
                eventId = UUID.randomUUID().toString,
                nodeId = currentNodeId,
                serialNumber = s.events.length+1,
                objectId = guid.toString,
                objectSize = objectSizeV,
                timestamp = arrivalTime+1,
                selectedNodeId = selectedNode.nodeId,
                milliSeconds = serviceTime
              )
              s.copy(events = s.events :+ missedEvent:+ newEvent)
            }
          }
          endAt               <- IO.realTime.map(_.toMillis)
          downloadServiceTime = endAt- arrivalTime
          rawBytes            <- response.body.compile.to(Array)
          _                   <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
          _                   <- ctx.logger.info(s"DOWNLOAD $selectedNodeId $guid $downloadServiceTime")
          _                  <- ctx.state.update{ s=>
            s.copy(events = s.events :+ Downloaded(
              eventId = UUID.randomUUID().toString,
              nodeId= currentNodeId,
              serialNumber=  s.events.length,
              objectId = guid.toString,
              objectSize= objectSizeV,
              timestamp = arrivalTime,
              selectedNodeId = selectedNode.nodeId,
              milliSeconds = downloadServiceTime
            ))
          }
          newResponse   <- Ok(fs2.Stream.emits(rawBytes).covary[IO], newHeaders)
          } yield newResponse
//
        case ((None,None ))=> for {
          _            <- ctx.logger.debug(s"Object[$guid] NOT found and lb NOT found")
          newResponse <- NotFound()
        } yield newResponse
      }

      _ <- ctx.logger.debug("____________________________________________________")

    } yield response
    case authReq@POST -> Root / "upload"   as user => for {
      arrivalTime        <- IO.realTime.map(_.toMillis)
      currentNodeId      =ctx.config.nodeId
      currentState       <- ctx.state.get
      maybeLB            = currentState.uploadBalancer
//    calculate available resources from events
      events             = Events.filterEvents(currentState.events)
      arMap              = Events.toNodeX(events = events).map(x=>(x.nodeId->x)).toMap
//   _______________________________________________________________________________
      maybeARNodeX       = NonEmptyList.fromList(arMap.values.toList)
      req                = authReq.req
      headers            = req.headers
//    get object id
      objectId           = headers.get(CIString("Object-Id")).map(_.head.value).get
//    get object size
      objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
//   ______________________________________________________________________________________________
//      _                  <- ctx.logger.debug(s"OBJECT_ID $objectId")
//      _                  <- ctx.logger.debug(s"OBJECT_SIZE $objectSize")
//   _____________________________________________________________________
      response           <- (maybeLB,maybeARNodeX) match {
        //       _______________________________________
        case (Some(lb),Some(nodes)) => for {
//          _             <- ctx.logger.debug("LB -> 1 - NODES ->1")
          _             <- IO.unit
          maybeNode     = lb.balance(objectSize = objectSize,nodes=nodes)
          response      <- maybeNode match {
            case Some(node) => for {
              response      <- Helpers.redirectTo(node.httpUrl,req)
              payload       <- response.as[PushResponse]
              _             <- Helpers.uploadSameLogic(node, currentNodeId, objectId, objectSize, arrivalTime, response)
              endAt         <- IO.realTime.map(_.toMillis)
              time          = endAt - arrivalTime
              _             <- ctx.logger.info(s"UPLOAD ${node.nodeId} ${payload.guid} ${payload.objectSize} $time")
            } yield response
            case None => InternalServerError()
          }
        } yield response
//      ________________________________
        case (None,Some(nodes)) => for {
//          _                 <- ctx.logger.debug("LB -> 0 - NODES -> 1")
          lb                <- Helpers.initLoadBalancerV3()
          maybeSelectedNode = lb.balance(objectSize = objectSize,nodes)
          response          <- maybeSelectedNode match {
            case Some(node) => for {
              response      <- Helpers.redirectTo(node.httpUrl,req)
              payload       <- response.as[PushResponse]
              _             <- Helpers.uploadSameLogic(node, currentNodeId, objectId, objectSize, arrivalTime, response)
              endAt         <- IO.realTime.map(_.toMillis)
              time          = endAt - arrivalTime
              _             <- ctx.logger.info(s"UPLOAD ${node.nodeId} ${payload.guid} ${payload.objectSize} $time")
            } yield response
            case None => InternalServerError()
          }
        } yield response
//      There are not AR
//      _______________________________________________
        case (Some(lb),None) => for {
          _ <- ctx.logger.debug("LB -> 1 - NODES -> 0")
          response <- ServiceUnavailable()
        } yield response
        case (None,None) => for {
          _ <- ctx.logger.debug("LB -> 0 - NODES -> 0")
//         INIT LOAD BALANCER
          lb                <- Helpers.initLoadBalancerV3()
//
          systemRepURI      = ctx.config.systemReplication.apiUrl+"/create/cache-node"
          systemRepPayload  = Json.obj(
            "poolId" -> ctx.config.poolId.asJson,
            "cacheSize" -> 2.asJson,
            "policy" -> "LFU".asJson,
            "basePort" -> 6000.asJson
          )
          systemRepReq       = Request[IO](method = Method.POST,uri = Uri.unsafeFromString(systemRepURI)).withEntity(systemRepPayload)
          (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
          systemRepResponse  <- client.expect[Json](systemRepReq)
          _                  <- ctx.logger.debug(systemRepResponse.toString)
          _                  <- finalizer
//          createNodeRes     <-
          response <- Ok("IT WORKS")
//            ServiceUnavailable()
        } yield response
      }
//
//      _            <- ctx.logger.debug("RESPONSE "+response.toString())
//      serviceTime <- IO.realTime.map(_.toMillis).map( _ - arrivalTime)
//      _           <- ctx.logger.info(s"UPLOAD $objectId $objectSize $serviceTime")
      _           <- ctx.logger.debug("____________________________________________________")
    } yield response
  }


  private def httpApp()(implicit ctx:NodeContext): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/api/v6" ->  authMiddleware(ctx=ctx)(authRoutes()),
      "/api/v6/add-node" -> AddNode(),
      "/api/v6/stats" -> HttpRoutes.of[IO]{
        case GET -> Root => for {
          currentState       <- ctx.state.get
          events             = Events.filterEvents(events = currentState.events)
          ars                = Events.toNodeX(events)
          distributionSchema = Events.generateDistributionSchema(events = events)
          objectsIds         = Events.getObjectIds(events = events)
          hitCounter         = Events.getHitCounterByNode(events = events)
          hitRatioInfo       = Events.getGlobalHitRatio(events=events)
          tempMatrix         = Events.generateTemperatureMatrixV2(events = events)
          nodeIds            = Events.getNodeIds(events = events)
          stats              = Map(
            "nodeId" -> ctx.config.nodeId.asJson,
            "poolId" -> ctx.config.poolId.asJson,
            "port"  -> ctx.config.port.asJson,
            "ipAddress" -> currentState.ip.asJson,
            "availableResources" -> ars.asJson,
            "distributionSchema" -> distributionSchema.asJson,
            "objectIds" -> objectsIds.sorted.asJson,
            "nodeIds" -> nodeIds.asJson,
            "hitCounterByNode"-> hitCounter.asJson,
            "tempMatrix" -> tempMatrix.asJson,
            "hitInfo" -> hitRatioInfo.asJson
          )
          response <- Ok(stats.asJson)
        } yield response
      },
      "/api/v6/events" -> HttpRoutes.of[IO]{
        case GET -> Root => for {
          currentState <- ctx.state.get
          rawEvents    = currentState.events
          events       = EventXOps.OrderOps.byTimestamp(rawEvents)
//          _            <- ctx.logger.debug(events.asJson.toString)
          response     <- Ok(events.asJson)
        } yield response
      }
    ).orNotFound

  def run()(implicit ctx:NodeContext): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp())
      .serve
      .compile
      .drain
  } yield ()
}
