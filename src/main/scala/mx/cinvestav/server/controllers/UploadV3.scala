package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContext
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import mx.cinvestav.commons.types.ReplicationSchema
import org.http4s.circe.CirceEntityDecoder._
import io.circe.syntax._
import io.circe.generic.auto._


object UploadV3 {


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "upload" =>
      for {
        currentState <- ctx.state.get
        payload      <- req.as[ReplicationSchema]
        response     <- Ok()
      } yield response
  }

}
