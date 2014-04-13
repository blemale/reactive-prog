package quickcheck

import common._
import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == Math.min(a, b)
  }

  property("del1") = forAll { a: Int =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }

  property("meld1") = forAll { (a: H, b: H) =>
    val h = meld(a, b)
    findMin(h) == Math.min(findMin(a), findMin(b))
  }

  property("min3") = forAll { h: H =>
    val l = toList(h)
    l == l.sorted
  }

  property("meld2") = forAll { (a: H, b: H) =>
    val l = toList(meld(a, b))
    l == l.sorted
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("min4") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(deleteMin(h)) == Math.max(a, b)
  }

  property("min4") = forAll { l : List[Int] =>
    val h = fromList(l)
    toList(h) == l.sorted
  }

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(value(empty), genHeap)
  } yield insert(v, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  def toList(h: H): List[Int] = {
    if (isEmpty(h)) Nil
    else findMin(h) :: toList(deleteMin(h))
  }

  def fromList(l: List[Int]): H = l match {
    case Nil => empty
    case x :: xs => insert(x, fromList(xs))
  }

}
