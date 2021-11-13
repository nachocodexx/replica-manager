import cats.effect.IO
import fs2.io.file.Files
import io.circe.{Decoder, HCursor}
import mx.cinvestav.commons.events.{AddedNode, Downloaded, EventX, EventXOps, Evicted, Get, Missed, Put, Uploaded}
import mx.cinvestav.events.Events

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import javax.naming.event.EventContext
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

class EventParserSpect extends munit .CatsEffectSuite {

  implicit val eventDecoderX:Decoder[EventX] = (hCursor:HCursor) =>{
    for {
      eventType <- hCursor.get[String]("eventType")
      decoded   <- eventType match {
        case "EVICTED" => hCursor.as[Evicted]
        case "UPLOADED" => hCursor.as[Uploaded]
        case "DOWNLOADED" => hCursor.as[Downloaded]
        case "PUT" => hCursor.as[Put]
        case "GET" => hCursor.as[Get]
        case "MISSED" => hCursor.as[Missed]
        case "ADDED_NODE" => hCursor.as[AddedNode]
      }
    } yield decoded
  }
  test("Basics"){
    val eventsString = Files[IO]
      .readAll(Paths.get("/home/nacho/Programming/Scala/experiments-suite/target/source/LFU_5-10_ex0" +
        ".json"),8192)
      .compile
      .to(Array)
      .map(new String(_,StandardCharsets.UTF_8))

    eventsString
      .flatMap{ inputString=>
        io.circe.parser.decode[List[EventX]](inputString) match {
          case Left(value) =>
            IO.delay{List.empty[EventX]} *> IO.println(s"ERROR: $value")
          case Right(events) =>
            val filteredEvents = Events.filterEvents(events=EventXOps.OrderOps.byTimestamp(events).reverse )
            val x= Events.getHitCounterByNodeV2(events = filteredEvents)
              .map{
              case (nodeId, counter) =>
                nodeId -> counter.filter(_._2>0)
            }
            val y = x.map{
              case (str, value) => str -> value.toList.length
            }
            val z= Events.nodesToObjectSizes(events = filteredEvents).map{
              case (str, value) => str -> value.length
            }
//              .getAllNodeXs(events = filteredEvents)
//              .filter(_._2.filter(_._2>0))
            for {
//            _ <- IO.println(x.asJson)
//              _ <- IO.println(y.asJson)
              _ <- IO.println(z.asJson)
            } yield ()
//            IO.pure(events)
        }
      }
//      .flatMap(IO.println)
  }

}
