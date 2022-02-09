package mx.cinvestav

import cats.effect.IO
import fs2.Stream
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.AddedNode
import mx.cinvestav.events.Events
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import scala.concurrent.duration.FiniteDuration

object Daemon {
  def apply(period:FiniteDuration)(implicit ctx:NodeContext): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](period).evalMap{ _ =>
      for {
        currentState  <- ctx.state.get
        events        = Events.orderAndFilterEventsMonotonic(events=currentState.events)
        addedServices = Events.onlyAddedNode(events=events).map(_.asInstanceOf[AddedNode])
        responses     <- Helpers.getNodesInfos(addedServices = addedServices)
//        _ <- ctx.logger.debug(currentState.infos.asJson.toString())
        _             <- ctx.state.update(_.copy(infos =  responses))
      } yield()
    }
  }

}
