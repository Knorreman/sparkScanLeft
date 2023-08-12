package ConditionalTail

import org.scalatest.funsuite.AnyFunSuiteLike
import ConditionalTail._
class ConditionalTailTest extends AnyFunSuiteLike {

  test("testConditionalTail_true") {
    val tail: List[Int] = (0 to 4).toList.tail
    val expected = List(1, 2, 3, 4)

    assert(tail.equals(expected))

    val tailIf: List[Int] = (0 to 4)
      .toList
      .tailIf(true)

    assert(tailIf.equals(expected))
  }

  test("testConditionalTail_false") {
    val expected = List(1, 2, 3, 4)

    val tailIf: List[Int] = expected
      .tailIf(false)

    assert(tailIf.equals(expected))
  }

  test("testConditionalTailSeq_true") {

    val seq = Seq(0, 1, 2, 3, 4)

    val tail: Seq[Int] = seq.tail
    val expected = List(1, 2, 3, 4)

    assert(tail.equals(expected))

    val tailIf: Seq[Int] = seq
      .tailIf(true)

    assert(tailIf.equals(expected))
  }

  test("testConditionalTailSeq_false") {
    val expected = Seq(1, 2, 3, 4)

    val tailIf: Seq[Int] = expected
      .tailIf(false)

    assert(tailIf.equals(expected))
  }
}
