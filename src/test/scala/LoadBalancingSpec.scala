import cats.Order
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeX
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
class LoadBalancingSpec extends munit .CatsEffectSuite {

  test("Basic"){
    val n0 = NodeX("n0","localhost",0,Map("level"->"0"))
    val n1 = NodeX("n0","localhost",0,Map("level"->"0"))
    val n2 = NodeX("n0","localhost",0,Map("level"->"1"))
    implicit val nodexOrder = new Order[NodeX]{
      override def compare(x: NodeX, y: NodeX): Int = 0
    }
    val nodes = NonEmptyList.of(n0,n1,n2)
    val lvl0 = nodes.filter(_.metadata.get("level").flatMap(_.toIntOption).getOrElse(Int.MaxValue) ==0)
    val lvl1 = nodes.filter(_.metadata.get("level").flatMap(_.toIntOption).getOrElse(Int.MaxValue) ==1)
    println(lvl0)
    println(lvl1)
    val lb = LoadBalancer("RB",nodes)
    val res = lb.balance(2)
    println(res)

  }

}
