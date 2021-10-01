package mx.cinvestav.handlers

import cats.data.EitherT
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.model.AmqpEnvelope
import mx.cinvestav.Declarations.{NodeState, NodeX}
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.liftFF
import org.typelevel.log4cats.Logger
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.payloads.v2.AddNode
//

object AddNodeHandler {
//  case class Payload(nodeId:String,ip:String,port:Int,timestamp:Long)
//
  def apply()(implicit ctx:NodeContext,envelope:AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    type E                = NodeError
    val maybeCurrentState = EitherT.liftF[IO,E,NodeState](ctx.state.get)
    implicit val logger   = ctx.logger
    val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
    val L                 = Logger.eitherTLogger[IO,E]
    def successCallback(payload:AddNode) = {
      val app = for {
        currentState <- maybeCurrentState
        timestamp    <- liftFF[Long,E](IO.realTime.map(_.toMillis))
        nodeXId      = payload.nodeId
        level        = payload.metadata.get("level").flatMap(_.toIntOption).getOrElse(0)
        newNode      = (nodeXId -> NodeX(
          nodeId= nodeXId,
          ip=payload.ip,
          port=payload.port,
          metadata = payload.metadata
        ))
        _            <-  level match {
          case 0 => for {
            _ <- L.debug("ADD TO LEVEL 0")
            _            <- liftFF[Unit,E](
              ctx.state.update(s=>s.copy(nodesLevel0 = s.nodesLevel0 + newNode))
            )
          } yield ()
          case 1 => for {
            _ <- L.debug("ADD TO LEVEL 1")
            _ <- liftFF[Unit,E](
              ctx.state.update(s=>s.copy(nodesLevel1 = s.nodesLevel1 + newNode))
            )
          } yield ()
        }
        latency      =  timestamp - payload.timestamp
        _            <- L.info(s"ADD_NODE_LATENCY $latency")
        _            <- L.debug(payload.asJson.toString)
//
      } yield ()
//
      app.value.stopwatch.flatMap { res =>
        res.result match {
          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) => for {
            _ <- acker.ack(envelope.deliveryTag)
            _ <- ctx.logger.debug(s"ADD_NODE ${res.duration.toMillis}")
          } yield ()
        }
      }
    }
    processMessageV2[IO,AddNode,NodeContext](
      successCallback =  successCallback,
      errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }

}
