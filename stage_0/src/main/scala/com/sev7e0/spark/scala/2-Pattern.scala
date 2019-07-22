package com.sev7e0.spark.scala

object Pattern {
  def main(args: Array[String]): Unit = {

    def result(): Seq[(String, Int)] = ("file", 5) :: ("edit", 10) :: ("navigate", 50) :: Nil

    forPattern(result())


    val list = List(1, 2, 3) :: List.empty :: List(6, 9) :: Nil
    forList(list)


    val tuples = ("this", 2) :: ("is", 10) :: ("a", 30) :: ("test", 70) :: Nil
    wordsOutliers(tuples)
    wordsOutliersWithCollect(tuples)
    wordsOutliersWithCollect2(tuples)
  }

  /**
    * for 语句中，生成器的左侧也可以是模式。 从而，可以直接在左则把想要的值解构出来
    *
    * @param result
    */
  def forPattern(result: Seq[(String, Int)]): Unit = {
    def res = for {
      (name, score) <- result
      //如果不使用守卫将会匹配所有
      if score > 5
    } yield name

    for (elem <- res) {
      println(elem)
    }
  }

  /**
    * 左侧的模式不匹配空列表。 这不会抛出 MatchError ，但对应的空列表会被丢掉，因此得到的结果是 List(3, 2)
    *
    * @param list
    * @return
    */
  def forList(list: Seq[List[Int]]): Seq[Int] = {
    val size = for {
      list@_ :: _ <- list
    } yield list.size
    println(size)
    size
  }

  /**
    * 使用模式匹配的匿名函数
    *
    * @param seq
    * @return
    */
  def wordsOutliers(seq: Seq[(String, Int)]): Seq[String] = {
    val result = seq.filter { case (_, s) => s > 2 && s < 70 } map { case (w, _) => w }
    println(s"result = $result")
    result
  }

  /**
    * 使用偏函数替代 filter 和 map 亦可以自己定义偏函数
    *
    * @param seq
    * @return
    */


  def wordsOutliersWithCollect(seq: Seq[(String, Int)]): Seq[String] = {
    val result = seq.collect { case (w, n) if n > 2 && n < 70 => w }
    println(s"get result with collect = $result")
    result
  }

  //自定义偏函数
  /**
    * 偏函数的作用是对于输入序列的每一个元素，先检查其在偏函数上是否有定义，如果没有则直接忽略，
    * 如果有定义那么就应用偏函数到这个元素上（也就是说使用过滤条件），对于满足的直接加入结果集。
    */
  val pf: PartialFunction[(String, Int), String] = {
    case (word, freq) if freq > 2 && freq < 70 => word
  }

  def wordsOutliersWithCollect2(seq: Seq[(String, Int)]): Seq[String] = {
    val result = seq.collect(pf)
    println(s"get result with collect = $result")
    result
  }

}
