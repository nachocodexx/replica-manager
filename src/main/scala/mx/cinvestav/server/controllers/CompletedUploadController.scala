package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{Get, GetCompleted, Put, PutCompleted}
import mx.cinvestav.events.Events
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
//{->, /, InternalServerError, NoContent, NotFound, POST, Root}
import mx.cinvestav.commons.events.EventXOps

object CompletedUploadController {


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{

    //    __________________________________________________________________________________
    case req@POST -> Root / "upload" / operationId / objectId =>
      val program = for {
        currentState <- ctx.state.get
        //      ________________________________________________________________________
        events       = Events.filterEventsMonotonicV2(events = currentState.events)
        puts         = Events.onlyPutos(events = events).map(_.asInstanceOf[Put])
        maybePut     = puts.find(p => p.correlationId == operationId && p.objectId == objectId)
        //      ________________________________________________________________________
        res          <- maybePut match {
          case Some(put) => for {
            res          <- NoContent()
            completedPut = PutCompleted.fromPut(p = put)
            _            <- Events.saveEvents(events = completedPut::Nil)
          } yield res
          //          ________________________________________________________________________
          case None => NotFound()
        }
      } yield res
      program.handleErrorWith{ e =>
        ctx.logger.error(e.getMessage) *> InternalServerError()
      }


    case req@POST -> Root / "download" / operationId / objectId =>
      val program = for {
        currentState <- ctx.state.get
        //      ________________________________________________________________________
        events       = Events.filterEventsMonotonicV2(events = currentState.events)
        gets         = EventXOps.onlyGets(events = events).map(_.asInstanceOf[Get])
        maybePut     = gets.find(p => p.correlationId == operationId && p.objectId == objectId)
        //      ________________________________________________________________________
        res          <- maybePut match {
          case Some(put) => for {
            res          <- NoContent()
            completedPut = GetCompleted.fromPut(p = put)
            _            <- Events.saveEvents(events = completedPut::Nil)
          } yield res
          //          ________________________________________________________________________
          case None => NotFound()
        }
      } yield res
      program.handleErrorWith{ e =>
        ctx.logger.error(e.getMessage) *> InternalServerError()
      }
  }

}
