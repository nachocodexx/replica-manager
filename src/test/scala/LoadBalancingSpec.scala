import cats.Order
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeX
import mx.cinvestav.commons.balancer.v2.{Balancer, LoadBalancer}
class LoadBalancingSpec extends munit .CatsEffectSuite {

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
    val n0 = NodeX("n0","localhost",4000,Map("level"->"0"))
    val n1 = NodeX("n1","localhost",4001,Map("level"->"0"))
    val n2 = NodeX("n2","localhost",4002,Map("level"->"1"))
    val n3 = NodeX("n3","localhost",4003,Map("level"->"0"))
//   ___________________
    implicit val nodexOrder = new Order[NodeX]{
      override def compare(x: NodeX, y: NodeX): Int = Order[Int].compare(x.port,y.port)
    }
//   _________
    val nodes = NonEmptyList.of(n0,n1,n2)
    val lb    = LoadBalancer("LC",nodes)
    val res   = lb.balance(2)
    val newNodes = NonEmptyList.of(n0,n1,n2,n3)
    val sampleCoutner = newNodes.map(x=>(x,0)).toNem
    val lastCounter = lb.getCounter
    val newCounter = sampleCoutner |+| lastCounter
    val newRes     = lb.balanceWithCounter(1,newCounter)
    println(res)
    println(newRes)
    println(lb.getCounter)

  }

}
