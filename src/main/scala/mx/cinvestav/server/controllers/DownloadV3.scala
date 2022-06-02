package mx.cinvestav.server.controllers

import mx.cinvestav.Declarations.NodeContext
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.typelevel.ci.CIString
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.operations.Operations

object DownloadV3 {

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@GET -> Root / "download" / objectId  => for {
        _ <- s.acquire
        currentState <- ctx.state.get
        headers      = req.headers
        technique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse(ctx.config.replicationTechnique)
        ds           = Operations.distributionSchema(
          operations          = currentState.operations,
          completedOperations = currentState.completedOperations,
          technique           = technique
        )
        maybeObject  = ds.get(objectId)
        response     <- maybeObject match {
          case Some(replicaNodes) => for {
            _ <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
            response     <- Ok()
          } yield response
          case None => NotFound()
        }
        _ <- s.release
      } yield response
  }


}
