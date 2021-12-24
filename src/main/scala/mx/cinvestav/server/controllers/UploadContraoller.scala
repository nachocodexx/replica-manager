package mx.cinvestav.server.controllers

import retry._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.Put
import org.http4s.Response
//
import mx.cinvestav.Declarations.{NodeContext, User}
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.Uploaded
import mx.cinvestav.events.Events
import mx.cinvestav.server.HttpServer.PushResponse
//
import java.util.UUID
//
import org.http4s.{Header, Headers}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.{AuthedRoutes, Method, Request, Uri}
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.Json
import org.typelevel.ci.CIString
//
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import language.postfixOps

object UploadContraoller {



  def apply()(implicit ctx:NodeContext): AuthedRoutes[User, IO] = {
    AuthedRoutes.of[User,IO]{

      case authReq@POST -> Root / "upload"   as user => for {
        arrivalTime        <- IO.realTime.map(_.toMillis)
        arrivalTimeNanos   <- IO.monotonic.map(_.toNanos)
        currentNodeId      = ctx.config.nodeId
        currentState       <- ctx.state.get
//        SEMAPHORE
//        _                 <- currentState.s.acquire
        maybeLB            = currentState.uploadBalancer
        //    calculate available resources from events
        rawEvents          = currentState.events
//        events             = Events.filterEvents(currentState.events)
//        events             = Events.orderAndFilterEvents(rawEvents)
        events             <- IO.delay{Events.orderAndFilterEventsMonotonic(rawEvents)}
//        arMap              = Events.getAllNodeXs(events = events).map(x=>x.nodeId->x).toMap
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
//        _                  <- ctx.logger.debug(maybeARNodeX.toString)
        //   _______________________________________________________________________________
        req                = authReq.req
        headers            = req.headers
        //    get object id
        objectId           = headers.get(CIString("Object-Id")).map(_.head.value).get
        maybeObject        = Events.getObjectById(objectId = objectId,events=events)
        operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        response           <- maybeObject match {
          case Some(_) =>  Forbidden()
          case None => for {
            _ <- IO.unit
            objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            //   ______________________________________________________________________________________________
            //   _____________________________________________________________________
            response           <- (maybeLB,maybeARNodeX) match {
               //       _______________________________________
               case (Some(lb),Some(nodes)) => for {
                 maybeNode     <- IO.delay{lb.balance(objectSize = objectSize,nodes=nodes)}
//                 _             <-
                 response      <- maybeNode match {
                   case Some(node) => for {
                     _           <- IO.unit
                     retryPolicy = RetryPolicies.limitRetries[IO](5) join RetryPolicies.exponentialBackoff[IO](2 seconds )
                     response             <- retryingOnAllErrors[Response[IO]](
                       policy = retryPolicy,
                       onError = (e:Throwable,d:RetryDetails)=> ctx.errorLogger.error(e.getMessage+s" $objectId")
                     )(Helpers.redirectTo(node.httpUrl,req))
//                     response      <- Helpers.redirectTo(node.httpUrl,req)
//                     payload       <- response.as[PushResponse]
                     _             <- Helpers.uploadSameLogic(
                       userId = user.id.toString,
                       nodeId = node.nodeId,
                       currentNodeId = currentNodeId,
                       objectId = objectId,
                       objectSize = objectSize,
//                       arrivalTime = arrivalTime,
                       arrivalTimeNanos = arrivalTimeNanos,
                       response = response,
                       correlationId = operationId
                     )
                   } yield response
                   case None => ctx.errorLogger.error(s"NO_NODES $objectId")*>InternalServerError()
                 }
               } yield response
               //      ________________________________
               case (None,Some(nodes)) => for {
                 lb                <- Helpers.initLoadBalancerV3(ctx.config.uploadLoadBalancer)
                 maybeSelectedNode <- IO.delay{lb.balance(objectSize = objectSize,nodes)}
                 response          <- maybeSelectedNode match {
                   case Some(node) => for {
                     _ <- IO.unit
                     retryPolicy = RetryPolicies.limitRetries[IO](5) join RetryPolicies.exponentialBackoff[IO](2 seconds )
                     response             <- retryingOnAllErrors[Response[IO]](
                       policy = retryPolicy,
                       onError = (e:Throwable,d:RetryDetails)=> ctx.errorLogger.error(e.getMessage+s" $objectId")
                     )(Helpers.redirectTo(node.httpUrl,req))
//                     response      <- Helpers.redirectTo(node.httpUrl,req)
                     _             <- Helpers.uploadSameLogic(
                       userId =  user.id.toString,
                       nodeId = node.nodeId,
                       currentNodeId = currentNodeId,
                       objectId = objectId,
                       objectSize = objectSize,
                       arrivalTimeNanos = arrivalTimeNanos,
                       response = response,
                       correlationId = operationId
                     )
                   } yield response
                   case None => InternalServerError()
                 }
               } yield response
//              ==============================================================================================================
               //      There are not AR
               //      _______________________________________________
               case (Some(lb),None) => for {
                  _ <- ctx.logger.debug("ERROR (_) - (_)")
                 response <- InternalServerError()
               } yield response
               case (None,None) => for {
                 _                 <- ctx.logger.debug("LB -> 0 - NODES -> 0")
                 lb                <- Helpers.initLoadBalancerV3(ctx.config.uploadLoadBalancer)
////                 systemRepResponseMaybe <- Helpers.createNode()
                 response  <- Ok()
//                 response         <- systemRepResponseMaybe match {
//                   case Some(systemRepResponse) => for {
//                     _                 <- ctx.logger.debug("HEREEE!")
//                     tryRedirectUpload =  Helpers.redirectTo(systemRepResponse.url,req)
//                     retryExponential  = RetryPolicies.limitRetries[IO](10) join RetryPolicies.exponentialBackoff[IO](baseDelay = 1 seconds)
//                     response          <- retryingOnAllErrors[Response[IO]](
//                       policy = retryExponential,
//                       onError=(e:Throwable,details:RetryDetails) =>{
//                         ctx.errorLogger.error(e.getMessage)*> ctx.errorLogger.error(details.toString)
//                       }
//                     )(tryRedirectUpload)
//
//                     _ <- ctx.logger.debug(response.toString())
//                     _             <- Helpers.uploadSameLogic(
//                       userId =  user.id.toString,
//                       nodeId = systemRepResponse.nodeId,
//                       currentNodeId = currentNodeId,
//                       objectId = objectId,
//                       objectSize = objectSize,
//                       arrivalTimeNanos = arrivalTimeNanos,
//                       response = response,
//                       correlationId = operationId
//                     )
//
//                   } yield response
//                   case None => ctx.errorLogger.error("ERROR_CREATED_NODE") *> InternalServerError()
//                 }
               } yield response
             }


            responseHeader = response.headers
            responseNodeId = responseHeader.get(CIString("Node-Id")).map(_.head.value).get
            responseLevel  = responseHeader.get(CIString("Level")).map(_.head.value).get
            endAtNanos    <- IO.monotonic.map(_.toNanos)
            uploadServiceTimeNanos     = endAtNanos - arrivalTimeNanos
            _             <- ctx.logger.info(s"UPLOAD $responseNodeId $objectId $objectSize $uploadServiceTimeNanos $responseLevel $operationId")
            _             <- ctx.logger.debug("____________________________________________________")
          } yield response

        }
        //        SEMAPHORE
//        _                 <- currentState.s.release.delayBy(300 milliseconds)
//        _ <- ctx.logger.debug("RELEASE")
      } yield response

    }
  }

}
