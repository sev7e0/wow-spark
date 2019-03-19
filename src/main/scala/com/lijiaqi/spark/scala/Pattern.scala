package com.lijiaqi.spark.scala

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-19 15:03
  **/
object Pattern {
  def main(args: Array[String]): Unit = {
    def result(): Seq[(String, Int)] = ("file", 5) :: ("edit", 10) :: ("navigate", 50) :: Nil

    val list = List(1,2,3)::List.empty::List(6,9)::Nil

    forPattern(result())

    forList(list)
  }

  /**
    *for 语句中，生成器的左侧也可以是模式。 从而，可以直接在左则把想要的值解构出来
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
    * @param list
    * @return
    */
  def forList(list: Seq[List[Int]]): Seq[Int] ={
    val size =for {
      list @ head::_ <- list
    }yield list.size
    println(size)
    size
  }

}
