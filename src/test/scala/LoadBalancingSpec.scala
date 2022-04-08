import cats.Order
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.types.{NodeX, PendingReplication}
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
import org.http4s.{Header, Headers}
import org.typelevel.ci.CIString

import scala.concurrent.duration._
import language.postfixOps
class LoadBalancingSpec extends munit .CatsEffectSuite {

  test("K"){
    val ps = List(
      PendingReplication(rf = 0,objectId = "o0",objectSize = 0L),
      PendingReplication(rf = 1,objectId = "o0",objectSize = 0L),
      PendingReplication(rf = 2,objectId = "o0",objectSize = 0L),
    )
    val ps0 = ps.filter(_.rf>0)
    println(ps0)
//    val program = IO.println("PUTOS TODOS") *> IO.println("AAAAAA") *> IO.pure(scala.math.random())
//    program.replicateA(10).flatTap(xs=>IO.println(xs.toString))
  }
  test("M"){
//    val x = Map("a"->0,"b"->0,"c"->0)
//    val y = Map("a"->1,"c"->2)
//    println(y|+|x)
    val h1 = Headers(Header.Raw(CIString("A"),"1"))
    val h2 = Headers(Header.Raw(CIString("A"),"2"))
    val hs = List(h1,h2).map(_.headers).flatten
    val hss = Headers(hs)
//    val hss = hs.foldRight(Headers.empty)(_|+|_)
    println(hss)
  }
  test("O"){
    for {
      t0 <- IO.monotonic.map(_.toMillis)
      _ <- IO.sleep(1 seconds)
      t1 <- IO.monotonic.map(_.toMillis)
      x   = t1-t0
      _ <- IO.println(x)
    } yield ()
  }
  test("A"){
    val numbers = List(0,1)
    val letters = List("a","b","c","d")
    val z       = letters.zipWithIndex.map( x => (x._1, numbers(x._2 % numbers.length)) )
    println(z)
  }
  test("Basics0"){
    val op0 = Some(1)
    val op1:Option[Int] = Some(1)
    val res = op0 zip op1 match {
      case Some((x0,x1)) => x0+x1
      case None => 0
    }
    println(res)
  }

  test("Basic"){

  }

}
