package com.sev7e0.spark.scala

import scala.annotation.implicitNotFound

object TypeClass {

  def main(args: Array[String]): Unit = {
    val numbers = Vector[Int](13, 23, 42, 45, 61, 73, 96, 100, 199, 420, 900, 3839)
//    println(Match.Statistics.mean(numbers))
  }
}



object Match{
  object Statistics{
    /**
      * 在第二个参数中使用了implicit，表示在当前作用域中必须存在一个隐式可用的NumberLike[T]对象
      * 可以使用 implicit value这种方式逆行声明，大多数情况下都是引入指定包或对象下的隐式值
      *
      * 当且仅当没有发现其他隐式值时，编译器会在隐式参数类型的伴生对象中寻找。
      * 作为库的设计者，将默认的类型类实现放在伴生对象里意味着库的使用者可以轻易的重写默认实现，这正是库设计者喜闻乐见的。
      * 用户还可以为隐式参数传递一个显示值，来重写作用域内的隐式值。
      * @param sx
      * @param ev
      * @tparam T
      * @return
      */
    def mean[T](sx: Vector[T])(implicit ev: NumberLike[T]): T =ev.divide(sx.reduce(ev.plus(_,_)), sx.size)
  }
  @implicitNotFound("not found match type class")
  trait NumberLike[T] {
    def plus(x: T, y: T): T
    def divide(x: T, y: Int): T
    def minus(x: T, y: T): T
  }

  object NumberLike{

    implicit object NumberLikeDouble extends NumberLike[Double]{
      override def plus(x: Double, y: Double): Double = x + y

      override def divide(x: Double, y: Int): Double = x / y

      override def minus(x: Double, y: Double): Double = x - y
    }

    implicit object NumberLikeInt extends NumberLike[Int]{
      override def plus(x: Int, y: Int): Int = x + y

      override def divide(x: Int, y: Int): Int = x / y

      override def minus(x: Int, y: Int): Int = x - y
    }

  }
}
