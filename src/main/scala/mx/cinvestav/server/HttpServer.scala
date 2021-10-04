package mx.cinvestav.server

import cats.data.{Kleisli, NonEmptyList, OptionT}
import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.{ObjectId, ObjectNodeKey}
import mx.cinvestav.commons.balancer.v2.Balancer
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.Declarations.NodeX
import mx.cinvestav.Helpers
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.multipart.Multipart
import org.http4s.server.Router
//
import mx.cinvestav.Declarations.{NodeContext, User,nodeXOrder}
import mx.cinvestav.commons.balancer.v2.LoadBalancer

//
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.server.AuthMiddleware
import org.typelevel.ci._
//
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//
object HttpServer {
  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           milliSeconds:Long,
                           timestamp:Long,
                           level:Int
                         )
  def authUser()(implicit ctx:NodeContext):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(ctx.logger.debug("AUTH MIDDLEWARE"))
      headers    = req.headers
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
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
      schema        = currentState.schema
      objectId      = ObjectId(guid.toString)
      maybeObject   = schema.get(objectId)
      arMap         = currentState.AR
      maybeLB       = currentState.downloadLB
      req           = authReq.req
      response      <- (maybeObject,maybeLB) match {
//
        case ((Some(objectX),Some(lb)))=> for {
          _                     <- ctx.logger.debug(s"Object[$guid] found and lb found")
          lastCounter          = lb.getCounter
          subsetNodes          = objectX.traverse(arMap.get).get
          (newLB,selectedNode) =  Balancer.balanceOverSubset(lb,rounds=1,subsetNodes = subsetNodes)
          _                    <- ctx.state.update{ s=>s.copy(downloadLB = newLB.some)}
          response             <- Helpers.redirectTo(selectedNode.head.httpUrl,req)
          endAt                <- IO.realTime.map(_.toMillis)
          time                 = endAt- arrivalTime
          rawBytes             <- response.body.compile.to(Array)
          guidH                 = response.headers.get(CIString("guid")).map(_.head).get
          objectSizeH           = response.headers.get(CIString("Object-Size")).map(_.head).get
          levelH                = response.headers.get(CIString("Level")).map(_.head).get
          objectNodeIdH         = response.headers.get(CIString("Node-Id")).map(_.head).get
          newHeaders           = Headers(guidH,objectSizeH,levelH,objectNodeIdH)
          _                    <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
          _                    <- ctx.logger.info(s"DOWNLOAD $guid $time")

          newResponse <- Ok(
            fs2.Stream.emits(rawBytes).covary[IO],
            newHeaders
          )
//        } yield response
      } yield newResponse
              //
        case ((Some(objectX),None ))=> for {
          _             <- ctx.logger.debug(s"Object[$guid] found and lb NOT found")
//
          nodes         = objectX.traverse(arMap.get).get
          newLb         = LoadBalancer[NodeX]("LC",xs= nodes)
          _             <- ctx.state.update(s=>s.copy(downloadLB =  newLb.some))
//
          selectedNode    =  newLb.balance(1).head
          response        <- Helpers.redirectTo(selectedNode.httpUrl,req)
          guidH                 = response.headers.get(CIString("guid")).map(_.head).get
          objectSizeH           = response.headers.get(CIString("Object-Size")).map(_.head).get
          levelH                = response.headers.get(CIString("Level")).map(_.head).get
          objectNodeIdH         = response.headers.get(CIString("Node-Id")).map(_.head).get
          newHeaders           = Headers(guidH,objectSizeH,levelH,objectNodeIdH)

          endAt         <- IO.realTime.map(_.toMillis)
          time           = endAt- arrivalTime
          rawBytes    <- response.body.compile.to(Array)
          _           <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
          _           <- ctx.logger.info(s"DOWNLOAD $guid $time")
          newResponse <- Ok(fs2.Stream.emits(rawBytes).covary[IO],newHeaders)
//        } yield response
      } yield newResponse
              //
        case (None,Some(lb) ) => for {
          _          <- ctx.logger.debug(s"Object[$guid] NOT found and lb found")
          filteredAR = arMap.values.toList
            .filter{x=>
              val usedPages = x.metadata.getOrElse("usedPages", "0").toInt
              val cacheSize = x.metadata.getOrElse("cacheSize","0").toInt
              usedPages < cacheSize
            }
          _   <- ctx.logger.debug(s"FILTERED_AR $filteredAR")
          response   <- if(filteredAR.isEmpty){
            for {
              _                     <- ctx.logger.debug("NO EMPTY SPACE IN DATA CONTAINERS")
              subsetNodes           = arMap.values.toList.toNel.get
              (newLB,selectedNode)  =  Balancer.balanceOverSubset(lb,rounds=1,subsetNodes = subsetNodes)
              _                     <- ctx.state.update{ s=>
                s.copy(downloadLB = newLB.some)
              }
              response     <- Helpers.redirectTo(selectedNode.head.httpUrl,req)
              guidH                 = response.headers.get(CIString("guid")).map(_.head).get
              objectSizeH           = response.headers.get(CIString("Object-Size")).map(_.head).get
              levelH                = response.headers.get(CIString("Level")).map(_.head).get
              objectNodeIdH         = response.headers.get(CIString("Node-Id")).map(_.head).get
              newHeaders           = Headers(guidH,objectSizeH,levelH,objectNodeIdH)
              _ <- ctx.logger.debug(s"NES - RESPONSE -> $response")
              endAt        <- IO.realTime.map(_.toMillis)
              time         = endAt- arrivalTime
              rawBytes    <- response.body.compile.to(Array)
              _           <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
              _            <- ctx.logger.info(s"DOWNLOAD $guid $time")
              newResponse          <- Ok(
                fs2.Stream.emits(rawBytes).covary[IO],
                newHeaders
              )
            } yield newResponse
          }
          else  for {
            _                     <- ctx.logger.debug("THERE ARE SPACE IN DATACONTAINERS")
            subsetNodes           = NonEmptyList.fromListUnsafe(filteredAR)
            _                     <- ctx.logger.debug(s"SUBSET $subsetNodes")
            (newLB,selectedNode)  =  Balancer.balanceOverSubset(lb,rounds=1,subsetNodes = subsetNodes)
            _                     <- ctx.logger.debug(s"SELECTED_NODE $selectedNode")
            _                     <- ctx.state.update{ s=>s.copy(downloadLB = newLB.some)}
            _                     <- ctx.logger.debug("BEFORE REDIRECT")
            response              <- Helpers.redirectTo(selectedNode.head.httpUrl,req)
            _                     <- ctx.logger.debug(s"RESPONSE $response")
            endAt                 <- IO.realTime.map(_.toMillis)
            time                  = endAt- arrivalTime
            rawBytes              <- response.body.compile.to(Array)
            _                     <- ctx.logger.debug(s"RAW_BYTES ${rawBytes.length}")
            _                     <- ctx.logger.info(s"DOWNLOAD $guid $time")
            newResponse          <- Ok(
              fs2.Stream.emits(rawBytes).covary[IO]
            )
//          } yield response
        } yield newResponse
      } yield response
//
        case ((None,None ))=> for {
          _            <- ctx.logger.debug(s"Object[$guid] NOT found and lb NOT found")
          nodes        = arMap.values.toList
          newLb        = LoadBalancer[NodeX]("LC",xs= NonEmptyList.fromListUnsafe(nodes))
          _            <- ctx.state.update(s=>s.copy(downloadLB =  newLb.some))
          selectedNode =  newLb.balance(1).head
          response     <- Helpers.redirectTo(selectedNode.httpUrl,req)
          _            <- ctx.logger.debug(response.toString)
          endAt        <- IO.realTime.map(_.toMillis)
          time         = endAt- arrivalTime
          _            <- ctx.logger.info(s"DOWNLOAD $guid $time")
          newResponse  <- Ok(response.body.covary[IO])
        } yield newResponse
      }

      _ <- ctx.logger.debug("____________________________________________________")

    } yield response
//
    case authReq@POST -> Root / "upload"   as user => for {
      arrivalTime        <- IO.realTime.map(_.toMillis)
      currentState       <- ctx.state.get
      arMap              = currentState.AR
      maybeLB            = currentState.lb
      maybeARNodeX       = NonEmptyList.fromList(arMap.values.toList)
      req                = authReq.req
      headers            = req.headers
      commonLogic = (selectedNode:NodeX) =>{
        for {
          response      <- Helpers.redirectTo(selectedNode.httpUrl,req)
          payload       <- response.as[PushResponse]
           payloadGUID  = ObjectId(payload.guid)
          //
          responseHeaders  = response.headers
          maybeEvictedItem = responseHeaders.get(CIString("Evicted-Item")).map(_.head.value)
//          UPDATE USED PAGES  OF DATACONTAINER
          _             <- ctx.state.update(s=>{
            val selectedNodeId = selectedNode.nodeId
            val nodeMetadata   = selectedNode.metadata
            val cacheSize      = nodeMetadata("cacheSize").toInt
            val newMetadata    = nodeMetadata.updatedWith("usedPages")(
              x=>x.flatMap(_.toIntOption).map(_+1).map(x=>if(x>cacheSize) cacheSize else x ).map(_.toString).getOrElse("1").some
            )
            val updatedNode    = selectedNode.copy(metadata = newMetadata)
            s.copy(AR = s.AR.updated(selectedNodeId, updatedNode))
          }
          )
          //         Schema
          _ <- maybeEvictedItem match {
            case Some(evictedItemId) =>  for {
              _              <- IO.unit
              responseNodeId = payload.nodeId
              objectNodeKey  = ObjectNodeKey(evictedItemId,responseNodeId)
              _              <- ctx.state.update(s => {
//                  val newMapping         = (payloadGUID -> NonEmptyList.of(selectedNode.nodeId))
//                REMOVE FROM SCHEMA
                  val newSchema          = s.schema.removed(ObjectId(evictedItemId))
//                ADD NEW NODE ID TO SCHEMA
                  val newSchema1         = newSchema.updatedWith(payloadGUID){
                    opNodes=>
                      opNodes.map(xs=> (xs :+ selectedNode.nodeId).distinct )
                        .getOrElse(NonEmptyList.of(selectedNode.nodeId))
                        .some
                  }
                //                REMOVE FROM DOWNLAOD COUNTER
                  val newDownloadCounter = s.downloadCounter.removed(objectNodeKey)
                  s.copy(schema = newSchema1,downloadCounter = newDownloadCounter)
                }
              )
            } yield ()
            case None => for {
              _ <- ctx.state.update(s => {
                  val nodeObjectKey = ObjectNodeKey(payload.guid,selectedNode.nodeId)
//                  val newMapping = ( payloadGUID -> NonEmptyList.of(selectedNode.nodeId))
                  s.copy(
                    schema = s.schema.updatedWith(payloadGUID){ opNodes=>
                      opNodes.map(xs=> (xs :+ selectedNode.nodeId).distinct)
                        .getOrElse(NonEmptyList.of(selectedNode.nodeId))
                        .some
                    },
//                    downloadCounter = s.downloadCounter.updatedWith(nodeObjectKey){ opCounter=>
//                      opCounter.map(_+1).getOrElse(1).some
//                    }
                  )
                }
              )
            } yield ()
          }

          endAt         <- IO.realTime.map(_.toMillis)
          time          = endAt - arrivalTime
          _             <- ctx.logger.info(s"UPLOAD ${payload.guid} ${payload.objectSize} ${selectedNode.nodeId} $time")
        } yield response
      }
      response           <- (maybeLB,maybeARNodeX) match {
        //       _______________________________________
        case (Some(lb),Some(nodes)) => for {
          _             <- ctx.logger.debug("LB -> 1 - NODES ->1")
          lastCounter   = lb.getCounter
          _             <- ctx.logger.debug("LAST_COUNTER "+lastCounter.toString)
          sampleCounter = nodes.map(x=>(x,0)).toNem
          newCounter    = sampleCounter |+| lastCounter
          _             <- ctx.logger.debug("NEW_COUNTER "+newCounter.toString)
          selectedNode  =  lb.balanceWithCounter(1,newCounter).head
          response      <- commonLogic(selectedNode)
        } yield response
//      ________________________________
        case (None,Some(nodes)) => for {
          _             <- ctx.logger.debug("LB -> 0 - NODES -> 1")
          newLb         = LoadBalancer[NodeX]("LC",xs= nodes)
          _             <- ctx.state.update(s=>s.copy(lb =  newLb.some))
          selectedNode  =  newLb.balance(1).head
//          SAME
          response <- commonLogic(selectedNode)
//          response      <- Helpers.redirectTo(selectedNode.httpUrl,req)
//          payload       <- response.as[PushResponse]
//          _             <- ctx.logger.debug(payload.toString)
        } yield response
//      _______________________________________________
        case (Some(lb),None) => for {
          _ <- ctx.logger.debug("LB -> 1 - NODES -> 0")
          response <- ServiceUnavailable()
        } yield response
        case (None,None) => for {
          _ <- ctx.logger.debug("LB -> 0 - NODES -> 0")
          response <- ServiceUnavailable()
        } yield response
      }
//
      _                  <- ctx.logger.debug(response.toString())
      _ <- ctx.logger.debug("____________________________________________________")
    } yield response
    case authReq@POST -> Root / "update-schema" as user => for {
      currentState <- ctx.state.get
      schema          = currentState.schema
      req             = authReq.req
      headers         = req.headers
      maybeEvictedItemGuid = headers.get(CIString("Evicted-Item-Id")).map(_.head.value)
      _               <- maybeEvictedItemGuid match {
        case Some(value) => for {
          _               <- ctx.logger.debug("EVICTION")
          evictedItemGuid = ObjectId(value)
          newItemGuid     = ObjectId(headers.get(CIString("New-Item-Id")).map(_.head.value).get)
          nodeId          = headers.get(CIString("Node-Id")).map(_.head.value).get
          newSchema0      = schema.removed(evictedItemGuid)
          newSchema1      = newSchema0 .updatedWith(newItemGuid){ opNodes=>
            opNodes.map(xs=> (xs :+ nodeId).distinct )
              .getOrElse(NonEmptyList.of(nodeId))
              .some
          }
          _               <- ctx.state.update(s=>s.copy(schema = newSchema1))
          _               <- ctx.logger.debug(s"LAST_SCHEMA $schema")
          _               <- ctx.logger.debug(s"NEW_SCHEMA $newSchema1")
        } yield ()
        case None => for {
          _ <- ctx.logger.debug("NO EVICTION")
          newItemGuid     = ObjectId(headers.get(CIString("New-Item-Id")).map(_.head.value).get)
          nodeId          = headers.get(CIString("Node-Id")).map(_.head.value).get
          newSchema1      = schema.updatedWith(newItemGuid){ opNodes=>
            opNodes.map(xs=> (xs :+ nodeId).distinct )
              .getOrElse(NonEmptyList.of(nodeId))
              .some
          }
          _               <- ctx.state.update(s=>s.copy(schema = newSchema1))
          _               <- ctx.logger.debug(s"LAST_SCHEMA $schema")
          _               <- ctx.logger.debug(s"NEW_SCHEMA $newSchema1")
        } yield ()
      }
      response        <- Ok("OK")
      _ <- ctx.logger.debug("____________________________________________________")
    } yield response

  }
  private def httpApp()(implicit ctx:NodeContext): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/api/v6" ->  authMiddleware(ctx=ctx)(authRoutes()),
      "/api/v6/stats" -> HttpRoutes.of[IO]{
        case GET -> Root => for {
          currentState <- ctx.state.get
          stats        = Map(
            "NODE_ID" -> ctx.config.nodeId.asJson,
            "POOL_ID" -> ctx.config.poolId.asJson,
            "PORT"  -> ctx.config.port.asJson,
            "IP_ADDRESS" -> currentState.ip.asJson,
            "AVAILABLE_RESOURCES" -> currentState.AR.asJson,
            "SCHEMA" -> currentState.schema.map{
              case (id, value) => (id.value->value.toList)
            }.asJson
          )
          response <- Ok(stats.asJson)
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
