package com.sev7e0.spark.scala

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-18 23:29
  **/
object PatternMatch {
  def main(args: Array[String]): Unit = {
    matchTuple(1) //the guard let you can use same type

    matchTuple(2) //guardType equals 2

    matchTuple("a", 2, 3) //(this is match tuple a,2,3)

    val map = Map(1 -> 1)
    matchTuple(map) // you can see string type also match

    val map1 = Map("a" -> "b")// match到该类型时 不会被泛型Int限制
    matchTuple(map1) //you can see string type also match

    matchTuple("string") // String type be match string

    matchTuple(null) //any don't match can pass it
  }

  def matchTuple(expr: Any): Unit =
    expr match {
      //tuple匹配
      case (a, b, c) => println("this is match tuple " + a, b, c)
      //带类型的匹配。
      case str: String => println("String type be match " + str)
      // case map : Map[_, _] => println(map.size)
      //会产生编译警告，Int泛型在编译期会被擦除。
      case map1: Map[Int, Int] => println("you can see string type also match")
      //Array的泛型不会被擦除。
      case array: Array[String] => println("only String Array can pass")
      //在scala中模式匹配不可以使用相同的匹配条件，但可以使用守卫进行判断
      case guardType: Int if guardType == 1 => println("the guard let you can use same type")
      case guardType: Int if guardType == 2 => println("guardType equals 2")

      case _ => println("any don't match can pass it")
    }

}
