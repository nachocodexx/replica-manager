package mx.cinvestav.server.controllers

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.balancer.v3.Balancer
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.events.Events
import org.http4s.{AuthedRequest, AuthedRoutes, Header, Headers}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.typelevel.ci.CIString

import java.util.UUID
import concurrent.duration._
import language.postfixOps

object UploadControllerV2 {

  def controller(operationId:String,objectId:String)(authReq:AuthedRequest[IO,User],user:User)(implicit ctx:NodeContext) =
    for {
      arrivalTime        <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos   <- IO.monotonic.map(_.toNanos)
      currentNodeId      = ctx.config.nodeId
      currentState       <- ctx.state.get
      rawEvents          = currentState.events
      events             <- IO.delay{Events.orderAndFilterEventsMonotonicV2(rawEvents)}
      arMap              <- IO.delay{
        ctx.config.uploadLoadBalancer match {
          case "UF" | "TWO_CHOICES" => Events.getAllNodeXs(
            events =rawEvents.sortBy(_.monotonicTimestamp)
          ).map(x=> x.nodeId->x).toMap
          case "ROUND_ROBIN" | "PSEUDO_RANDOM" => Events.getAllNodeXs(events=events).map(x=>x.nodeId->x).toMap
          //          case "PSEUDO_RANDOM"  => Events.getAllNodeXs(events=events).map(x=>x.nodeId->x).toMap
        }
      }
      //       NO EMPTY LIST OF RD's
      maybeARNodeX       = NonEmptyList.fromList(arMap.values.toList)
      //   _______________________________________________________________________________
      req                = authReq.req
      headers            = req.headers
//      objectId           = headers.get(CIString("Object-Id")).map(_.head.value).get
      maybeObject        = Events.getObjectById(objectId = objectId,events=events)
//      operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
      objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      maybeLB            = currentState.uploadBalancer

      commonCode  = (lb:Balancer,nodes:NonEmptyList[NodeX]) => for {
        maybeSelectedNode <- IO.delay{lb.balance(objectSize = objectSize,nodes)}
        response          <- maybeSelectedNode match {
          case Some(node) => for {
            now <- IO.realTime.map(_.toMillis)
            _   <- Events.saveEvents(
              events =  List(
                Put(
                  serialNumber = 0,
                  objectId = objectId,
                  objectSize = objectSize,
                  timestamp = now,
                  nodeId = node.nodeId,
                  serviceTimeNanos = 0L,
                  userId =  user.id.toString,
                  correlationId = operationId,
                  monotonicTimestamp = 0L
                )
              )
            )
            serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
//            _                <- ctx.logger.info(s"UPLOAD $objectId ${node.nodeId} $serviceTimeNanos $operationId")
            nodeUri = if(ctx.config.returnHostname) s"http://${node.nodeId}:6666/api/v6/upload" else  s"${node.httpUrl}/api/v6/upload"
            res <- Ok(nodeUri,
              Headers(
                Header.Raw(CIString("Object-Size"),objectSize.toString) ,
                Header.Raw(CIString("Node-Id"),node.nodeId)
              )
            )
          } yield res
          case None => Forbidden()
        }
      } yield response

//      maybeObject        = Events.getObjectById(objectId = objectId,events=events)
      response <- maybeObject match {
        case Some(o) => for {
          _   <- IO.unit
          res   <- Events.generateDistributionSchema(events = events).get(o.objectId) match {
            case Some(nodes) => for {
              _     <- ctx.logger.debug(s"${o.objectId} ALREADY UPLOADED")
              node = Events.getNodeById(events=events,nodeId = nodes.head).get
              x = if(ctx.config.returnHostname) s"http://${node.nodeId}:6666" else  node.httpUrl
              nodeUri = s"$x/api/v6/upload"
              res <- Ok(nodeUri,Headers(
                Header.Raw(CIString("Already-Uploaded"),"true"),
                Header.Raw(CIString("Node-Id"),node.nodeId),
                Header.Raw(CIString("Object-Size"),objectSize.toString),
                Header.Raw(CIString("Download-Url"),s"$x/api/v6/download/${o.objectId}"),
              ))
            } yield res
            case None => ctx.logger.debug("NO DISTRIBUTION SCHEMA")*> NotFound()
          }

        } yield res
        case None => (maybeLB,maybeARNodeX) match {
            case (None,Some(nodes)) => for {
              _  <- ctx.logger.debug("NO LOADBALANCER!")
              lb  <- Helpers.initLoadBalancerV3(ctx.config.uploadLoadBalancer)
              res <- commonCode(lb,nodes)
            } yield res
            case (Some(lb),Some(nodes)) => commonCode(lb,nodes)
            case (None,None) => ctx.logger.debug("NONE") *> Forbidden()
          }
      }

    } yield response

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext)={
    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "uploadv2" as user => for {
        waitingTimeStartAt <- IO.monotonic.map(_.toNanos)
        _                  <- s.acquire
        operationId        = authReq.req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectId           = authReq.req.headers.get(CIString("Object-Id")).map(_.head.value).get
        waitingTimeEndAt   <- IO.monotonic.map(_.toNanos)
        waitingTime        = waitingTimeEndAt - waitingTimeStartAt
        _                  <- ctx.logger.info(s"WAITING_TIME $objectId 0 $waitingTime $operationId")
        response           <- controller(operationId,objectId)(authReq,user)
        headers            = response.headers
        selectedNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("X")
        _                  <- s.release
        serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - waitingTimeEndAt)
        _                  <- ctx.logger.info(s"UPLOAD $objectId $selectedNodeId $serviceTimeNanos $operationId")
        _                  <- s.release
        _ <- ctx.logger.debug("____________________________________________________")
      } yield response
    }
  }

}
