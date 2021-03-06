import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import fs2.io.file.Files
import fs2.hash
import mx.cinvestav.commons.fileX.FileMetadata
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.vault.Vault

import java.io.{ByteArrayOutputStream, File}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
//import org.http4s.client.dsl.io.
import concurrent.duration._

class UploadV6Spec extends munit .CatsEffectSuite {
  override def munitTimeout: Duration = Int.MaxValue seconds
  trait OperationType
  case object Upload extends OperationType{
    override def toString():String = "UPLOAD"
  }
  case object Download extends OperationType{

    override def toString():String = "DOWNLOAD"
  }
  case class RequestX(
                       operationType:OperationType,
                       arrivalTime:Long,
                       req:Request[IO]
                     )

  val resourceClient =  BlazeClientBuilder[IO](global).resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  final val TARGET   = "/home/nacho/Programming/Scala/load-balancing/target"
  final val SOURCE_FOLDER  = s"$TARGET/source"
  final val SINK_FOLDER  = s"/test/sink"
  final val pdf0File = new File(s"$SOURCE_FOLDER/0.pdf")
  final val pdf0Id   = UUID.fromString("8c729be1-f716-4ec0-b820-e8af649ec173")
  final val pdf1File = new File(s"$SOURCE_FOLDER/1.pdf")
  final val pdf1Id   = UUID.fromString("6434652f-3d07-4eba-ac33-278f04d577e1")
  final val pdf2File = new File(s"$SOURCE_FOLDER/2.pdf")
  final val pdf2Id   = UUID.fromString("2358408e-9d29-4b2e-b9a8-e5590ee0636d")
  final val pdf3File = new File(s"$SOURCE_FOLDER/3.pdf")
  final val pdf3Id   = UUID.fromString("9d60dae7-7527-4bc2-a180-22a2a1740ac7")
  final val pdf4File = new File(s"$SOURCE_FOLDER/4.pdf")
  final val pdf4Id   = UUID.fromString("db1de349-c836-4dd5-a874-39a3477cb441")
  final val pdf5File = new File(s"$SOURCE_FOLDER/00.pdf")
  final val pdf5Id   = UUID.fromString("506cb8b4-cedc-47a9-b319-34c9c82072a6")

  final val pdf6File = new File(s"$SOURCE_FOLDER/6.pdf")
  final val pdf6Id   = UUID.fromString("1bf34600-c155-44d7-8d52-799e83946b0d")
  final val pdf7File = new File(s"$SOURCE_FOLDER/7.pdf")
  final val pdf7Id   = UUID.fromString("048a4575-7893-4ed5-bd72-c56414aab3f2")
  final val pdf8File = new File(s"$SOURCE_FOLDER/8.pdf")
  final val pdf8Id   = UUID.fromString("af0a2cc9-208a-415e-8ef1-9d9141fcd295")

  final val pdf9File = new File(s"$SOURCE_FOLDER/9.pdf")
  final val pdf9Id   = UUID.fromString("e58d963b-6cc5-4f1a-8335-45d19561d494")
//
  final val mediumBookFile = new File(s"$SOURCE_FOLDER/medium_book.pdf")
  final val mediumBookId   = UUID.fromString("5e69b1c8-fdb4-469a-bfa2-aa7722ede4ce")
//
//  final val video0 = new File(s"$SOURCE_FOLDER/6.mkv")
  final val video0 = new File(s"$SOURCE_FOLDER/medium_video.mp4")
  final val video0Id   = UUID.fromString("c5d9d60b-ebde-4ae0-be32-40f5513af0a2")
//
  final val userId   = UUID.fromString("3acf3090-4025-4516-8fb5-fa672589b465")
  test("Mimetype"){
    val mt = MediaType.forExtension("jpg")
    val streamBytes = Files[IO].readAll(video0.toPath,8192)
    val buffer      = streamBytes.through{ s0=>
      fs2.Stream.suspend{
        s0.chunks.fold(new ByteArrayOutputStream(1000)){ (buffer,chunk)=>
          val bytes = chunk.toArraySlice
          buffer.write(bytes.values,bytes.offset,bytes.size)
          buffer
        }
      }
    }
    buffer
      .evalMap(buffer=>
//        buffer
        IO.println(buffer.size())
      )
      .compile.drain
//    println(mt)
  }

  test("Workload"){
//
    val downloadRequest = (port:Int,id:UUID,contentLength:Long) => Request[IO](
      method      = Method.GET,
      uri         = Uri.unsafeFromString(s"http://localhost:${port}/api/v6/download/$id"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers.empty,
      attributes  = Vault.empty
    )
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Object-Extension"),"pdf"),
          Header.Raw(CIString("Object-Size"),contentLength.toString),
//            MediaType.forExtension("pdf").getOrElse(MediaType.application.`octet-stream`)
//          ),
//          headers.`Content-Length`(contentLength)
        )
      )
//
    val parts:Vector[Part[IO]] = Vector(
      Part.fileData(name = "pdf0",file = pdf0File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf0Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf0File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf0File.length())
        )
      ),
      Part.fileData("pdf1",pdf1File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf1Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf1File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf1File.length())
        )
      ),
      Part.fileData("pdf2",pdf2File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf2Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf2File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf2File.length())
        )
      ),
      Part.fileData("pdf3",pdf3File,
      headers = Headers(
        Header.Raw(CIString("Object-Id"),pdf3Id.toString),
        Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf3File.toPath).fullname ),
        headers.`Content-Type`(MediaType.application.pdf),
        headers.`Content-Length`(pdf3File.length())
      )
      ),
      Part.fileData("pdf4",pdf4File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf4Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf4File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf4File.length())
        )
      ),

      Part.fileData("pdf5",pdf5File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf5Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf5File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf5File.length())
        )
      ),

      Part.fileData("pdf6",pdf6File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf6Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf6File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf6File.length())
        )
      ),
      Part.fileData("pdf7",pdf7File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf7Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf7File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf7File.length())
        )
      ),
      Part.fileData("pdf8",pdf8File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf8Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf8File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf8File.length())
        )
      ),
      Part.fileData("pdf9",pdf9File,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),pdf9Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf9File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf9File.length())
        )
      ),
//
      Part.fileData("video0",video0,
          headers = Headers(
            Header.Raw(CIString("Object-Id"),video0Id.toString),
            Header.Raw(CIString("filename"), FileMetadata.fromPath(video0.toPath).fullname ),
            headers.`Content-Type`(MediaType.video.mp4),
            headers.`Content-Length`(video0.length())
          )
    ),
      Part.fileData("pdf9",mediumBookFile,
        headers = Headers(
          Header.Raw(CIString("Object-Id"),mediumBookId.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(mediumBookFile.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(mediumBookFile.length())
        )
      ),

    )
    val multipartOneFile = (part:Part[IO]) => Multipart[IO](
      parts =  Vector(part),

    )
//
    val pdf0Multipart = multipartOneFile(parts(0))
    val pdf1Multipart = multipartOneFile(parts(1))
    val pdf2Multipart = multipartOneFile(parts(2))
    val pdf3Multipart = multipartOneFile(parts(3))
    val pdf4Multipart = multipartOneFile(parts(4))
    val pdf5Multipart = multipartOneFile(parts(5))
    val pdf6Multipart = multipartOneFile(parts(6))
    val pdf7Multipart = multipartOneFile(parts(7))
    val pdf8Multipart = multipartOneFile(parts(8))
    val pdf9Multipart = multipartOneFile(parts(9))
    val video0Multipart = multipartOneFile(parts(10))
    val mediumBookMultipart = multipartOneFile(parts(11))

    val uploadRequest = (port:Int,multipart:Multipart[IO]) =>Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"http://localhost:$port/api/v6/upload"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers = multipart.headers,
      attributes = Vault.empty
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Object-Id"),multipart.parts.head.headers.get(CIString("Object-Id")).map(_.head.value).get),
          Header.Raw(CIString("Object-Size"),multipart.parts.head.headers.get(CIString("Content-Length")).map(_.head.value).get),
//          headers.`Content-Type`(MediaType.forExtension("pdf").get)
        )
      )
    val replicateRequest = (port:Int,multipart:Multipart[IO]) =>Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"http://localhost:$port/api/v6/upload"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers = multipart.headers,
      attributes = Vault.empty
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          multipart.parts.head.headers.get(CIString("guid")).get
        )
      )


    val flushAllRequest = (port:Int,operationId:Int) =>Request[IO](
      method      = Method.POST,
      uri         = Uri.unsafeFromString(s"http://localhost:$port/api/v6/flush_all"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Operation-Id"),operationId.toString),
      ),
      attributes = Vault.empty
    )

    var lastArrivalTime = Long.MinValue
    resourceClient.use{ client => for {
      _      <- IO.unit
//
      trace  = List(
//        ________________________________________________________
//        RequestX(Upload,0,uploadRequest(3000,pdf0Multipart)),
        RequestX(Upload,0,uploadRequest(3000,pdf1Multipart)),
//        RequestX(Download,3200,downloadRequest(3000,pdf0Id,pdf0File.length())),
//            RequestX(Download,3200,downloadRequest(3000,pdf1Id,pdf1File.length())),
//          RequestX(Upload,3400,uploadRequest(3000,pdf2Multipart)),

//        RequestX(Download,3200,downloadRequest(3000,pdf5Id,pdf5File.length())),
//        RequestX(Upload,0,uploadRequest(3000,pdf1Multipart)),
//        RequestX(Download,4200,downloadRequest(3000,pdf2Id,pdf2File.length())),
//          RequestX(Upload,0,uploadRequest(3000,pdf2Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf3Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf4Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf5Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf6Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf7Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf8Multipart)),
//        RequestX(Upload,0,uploadRequest(3000,pdf9Multipart)),
//       RequestX(Download,4200,downloadRequest(3000,pdf2Id,pdf2File.length())),
//          RequestX(Download,5700,downloadRequest(3000,pdf5Id,pdf5File.length())),
//          RequestX(Download,5700,downloadRequest(3000,pdf5Id,pdf5File.length())),
      )

      responses <- trace.zipWithIndex.traverse {
        case (reqx, index) => for {
          _            <- IO.unit
          waitingTime_ = reqx.arrivalTime - lastArrivalTime
          waitingTime  = if(waitingTime_ < 0 )  0 else waitingTime_
          _            <- IO.sleep(waitingTime milliseconds)
          resultId     = UUID.randomUUID()
          initTime     <- IO.realTime.map(_.toMillis)
          res          <- client.stream(reqx.req).flatMap{ response=>
            val body = response.body
            if(reqx.operationType == Download){
              val sinkPath = Paths.get(SINK_FOLDER+s"/$resultId.pdf")
              body.through(Files[IO].writeAll(sinkPath)) ++ fs2.Stream.eval(IO.println(response))
            }
            else fs2.Stream.eval(IO.unit) ++ fs2.Stream.eval(IO.println(response))
          }.compile.drain
          endTime  <- IO.realTime.map(_.toMillis)
          time     = endTime-initTime
          _         <- IO.println(s"${reqx.operationType.toString} - $resultId - $time")
//          _        <-
          _ <- IO.delay{ lastArrivalTime = waitingTime }
        } yield res
      }
    } yield ()
    }


  }


}
//                VIDEO 0
//        RequestX(Upload,0,uploadRequest(3000,video0Multipart,0)),
//          RequestX(Download,100,downloadRequest(3000,video0Id)),
//        PDf 0
//          RequestX(Upload,0,uploadRequest(3000,pdf0Multipart,0)),
//        RequestX(Download,100,downloadRequest(3000,pdf0Id)),
//        RequestX(Upload,0,replicateRequest(3000,pdf0Multipart)),
////      PDF 1
//        RequestX(Upload,1500,uploadRequest(3000,pdf1Multipart,0)),
//        RequestX(Download,1100,downloadRequest(3000,pdf1Id)),
//
//        RequestX(Upload,17500,uploadRequest(3000,pdf2Multipart,0)),
//        RequestX(Download,17100,downloadRequest(3000,pdf2Id)),
////
//        RequestX(Upload,18500,uploadRequest(3000,pdf3Multipart,0)),
//        RequestX(Download,18100,downloadRequest(3000,pdf3Id)),
//      _____________________________________________________________
