import cats.Order
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.types.NodeX
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
class LoadBalancingSpec extends munit .CatsEffectSuite {

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
