package mx.cinvestav.handlers

import cats.effect.IO
import dev.profunktor.fs2rabbit.model.{AmqpFieldValue, QueueName}
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.utils.v2.Acker


object Handlers {

  def apply(queueName: QueueName)(implicit ctx:NodeContext): IO[Unit] = {
    val rabbitMQContext = ctx.rabbitMQContext
    val connection      = rabbitMQContext.connection
    val client          = rabbitMQContext.client
    client.createChannel(connection) .use { implicit channel =>
      for {
        _ <- ctx.logger.debug("START CONSUMING")
        (_acker, consumer) <- ctx.rabbitMQContext.client.createAckerConsumer(queueName = queueName)
        _ <- consumer.evalMap { implicit envelope =>
          val maybeCommandId = envelope.properties.headers.get("commandId")
          implicit val acker: Acker = Acker(_acker)
          maybeCommandId match {
            case Some(commandId) => commandId match {
              case AmqpFieldValue.StringVal(value) if value == "ADD_NODE" => AddNodeHandler()
              case AmqpFieldValue.StringVal(value) if value == "REMOVE_NODE"=> RemoveNodeHandler()
              case x =>
                ctx.logger.error(s"NO COMMAND_HANDLER FOR $x") *> acker.reject(envelope.deliveryTag)
            }
            case None => for{
              _ <- ctx.logger.error("NO COMMAND_ID PROVIED")
              _ <- acker.reject(envelope.deliveryTag)
            } yield ()
          }
        }.compile.drain
      } yield ()
    }
  }


}
