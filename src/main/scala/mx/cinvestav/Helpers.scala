package mx.cinvestav

//
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
//
import mx.cinvestav.Declarations.{NodeContext, NodeX, ObjectId, ObjectNodeKey}
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
import mx.cinvestav.commons.payloads.{CreateCacheNode, CreateCacheNodeResponse}
import mx.cinvestav.server.HttpServer.{PushResponse, ReplicationResponse}
//
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.typelevel.ci.CIString

import scala.collection.SortedSet
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import org.http4s._
//import org.http4s.implicits._
import org.http4s.blaze.client.BlazeClientBuilder
//
import concurrent.ExecutionContext.global

object Helpers {

  // Init new download load balancer
  def initDownloadLoadBalancer(loadBalancerType:String="LC",nodes:NonEmptyList[NodeX])(implicit ctx:NodeContext): IO[Balancer[NodeX]] = for {
    _      <- IO.unit
    newLb  = LoadBalancer[NodeX](loadBalancerType,xs= nodes)
    _      <- ctx.state.update(s=>s.copy(downloadLB =  newLb.some))
  } yield newLb
// Init new load balancer
  def initLoadBalancer(loadBalancerType:String="LC",nodes:NonEmptyList[NodeX])(implicit ctx:NodeContext): IO[Balancer[NodeX]] = for {
    _      <- IO.unit
    newLb  = LoadBalancer[NodeX](loadBalancerType,xs= nodes)
    _      <- ctx.state.update(s=>s.copy(lb =  newLb.some))
  } yield newLb

//  Update node metadata -> update schema
  def commonLogic(arrivalTime:Long,req:Request[IO])(selectedNode:NodeX,maybeGuid:Option[String])(implicit ctx:NodeContext): IO[Response[IO]] ={
      for {
        _              <- IO.unit
        headers            = req.headers
        newHeaders         = headers.headers.filter(x=>x.name.compare(CIString("guid")) != 0 )
        selectedNodeId = selectedNode.nodeId

        response       <- maybeGuid match {
          // Replica
          case Some(_) => Helpers.redirectTo(selectedNode.httpUrl,req.withHeaders(newHeaders))
          // Normal upload
          case None =>  Helpers.redirectTo(selectedNode.httpUrl,req)
        }

        payload        <- response.as[PushResponse]
        payloadGUID    = ObjectId(payload.guid)
        //
        responseHeaders  = response.headers
        maybeEvictedItem = responseHeaders.get(CIString("Evicted-Item")).map(_.head.value)

        //          UPDATE CAPACITY OF A CACHE NODE
        _ <- Helpers.updateNode(selectedNode= selectedNode)
        //         Update schema based on eviction
        _ <- maybeEvictedItem match {
          case Some(evictedItemId) =>  for {
            _              <- IO.unit
            responseNodeId = payload.nodeId
            objectNodeKey  = ObjectNodeKey(evictedItemId,responseNodeId)
            _              <- ctx.state.update(s => {
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
          //            NO EVICTION
          case None => Helpers.addToSchema(objectId = payloadGUID,selectedNodeId = selectedNodeId)
        }

        endAt         <- IO.realTime.map(_.toMillis)
        time          = endAt - arrivalTime
        _             <- ctx.logger.info(s"UPLOAD ${payload.guid} ${payload.objectSize} ${selectedNode.nodeId} $time")
      } yield response
    }

  def placeReplicaInAR(arrivalTime:Long,
                       arMap:Map[String,NodeX],
                       lb:Balancer[NodeX],
                       req:Request[IO]
                      )
                      (objectId: ObjectId,
                       availableResources:List[String])
                      (implicit ctx:NodeContext): IO[Response[IO]] =
    for {
      _                           <- ctx.logger.debug(s"THERE ARE FREE SPACES $availableResources")
      headers                     = req.headers
      maybeGuid                   = headers.get(CIString("guid")).map(_.head.value)
      subsetNodes                 = NonEmptyList.fromListUnsafe(availableResources.map(x=>arMap(x)))
      rf                          = 1
      (newBalancer,selectedNodes) = Balancer.balanceOverSubset(balancer = lb, rounds= rf ,subsetNodes =subsetNodes)
      _                           <- ctx.logger.debug(s"SELECTED_NODES $selectedNodes")
      _                           <- ctx.state.update(s=>s.copy(lb = newBalancer.some))
      fiber                       <- selectedNodes.traverse(Helpers.commonLogic(arrivalTime=arrivalTime,req=req)(_,maybeGuid))
      endAt                       <- IO.realTime.map(_.toMillis)
      milliSeconds                = endAt - arrivalTime
      payloadRss = ReplicationResponse(
        guid = objectId.value,
        replicas= selectedNodes.toList.map(_.nodeId),
        milliSeconds =  milliSeconds ,
        timestamp = arrivalTime
      )
      _res  <- Ok(payloadRss.asJson)
    } yield _res

  //  Create a SN and update the schema
  def createNodeAndPlaceReplica(objectId: ObjectId,nodesLength:Int)(implicit ctx:NodeContext) =
    for {
      _         <- ctx.logger.debug("CREATE A SYSTEM REPLICA")
      now       <- IO.realTime.map(_.toMillis)
      srPayload = CreateCacheNode(
        poolId = ctx.config.poolId,
        cacheSize = 10,
        policy="LRU",
        basePort = 2000
      )
      srReq    = Request[IO](
        method=Method.POST,
        uri = Uri.unsafeFromString(ctx.config.systemReplication.apiUrl+"/create/cache-node"),
        headers =  Headers(
          Header.Raw(CIString("Timestamp"),now.toString),
          Header.Raw(CIString("Pool-Node-Length"),nodesLength.toString)
        )
      ).withEntity(srPayload)

      createNodeFiber <- BlazeClientBuilder[IO](global).resource
        .use{ client =>client.expect[CreateCacheNodeResponse](srReq)}
        .flatMap{ resp=> Helpers.addToSchema(objectId = objectId,selectedNodeId = resp.nodeId )}.start
      _res            <-  Created()
    } yield _res

//  Add a new node to the current list of nodes F0 -> [...nodes]
    def addToSchema(objectId:ObjectId, selectedNodeId:String)(implicit ctx:NodeContext) = for {
        _ <- ctx.state.update(s => {
        s.copy(
          schema = s.schema.updatedWith(objectId){ opNodes=>
            opNodes.map(xs=> (xs :+ selectedNodeId).distinct).getOrElse(NonEmptyList.of(selectedNodeId)).some
          },
        )
      })
    } yield ()

    def updateNode(selectedNode: NodeX)(implicit ctx: NodeContext): IO[Unit] = ctx.state.update(s => {
      val selectedNodeId = selectedNode.nodeId
      val nodeMetadata = selectedNode.metadata
      val cacheSize = nodeMetadata("cacheSize").toInt
      val newMetadata = nodeMetadata.updatedWith("usedPages")(
        x => x.flatMap(_.toIntOption).map(_ + 1).map(x => if (x > cacheSize) cacheSize else x).map(_.toString).getOrElse("1").some
      )
      val updatedNode = selectedNode.copy(metadata = newMetadata)
      s.copy(AR = s.AR.updated(selectedNodeId, updatedNode))
    }
    )

    def redirectToWithClient(client:Client[IO])(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
      newReq = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
      response <- client.toHttpApp.run(newReq)
    } yield response

    def redirectTo(nodeUrl: String, req: Request[IO]): IO[Response[IO]] = for {
      _ <- IO.unit
  //    newReq = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
      (client, finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
      response            <- redirectToWithClient(client)(nodeUrl=nodeUrl,req=req)
  //    response <- client.toHttpApp.run(newReq)
      _ <- finalizer
    } yield response
}

