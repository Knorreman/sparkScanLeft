package ConditionalTail

object ConditionalTail {
  implicit class ConditionalTailSeq[A](seq: Seq[A]) {
    def tailIf(condition: Boolean): Seq[A] = {
      if (condition && seq.nonEmpty) seq.tail else seq
    }
  }

  implicit class ConditionalTailList[A](seq: List[A]) {
    def tailIf(condition: Boolean): List[A] = {
      if (condition && seq.nonEmpty) seq.tail else seq
    }
  }
}
