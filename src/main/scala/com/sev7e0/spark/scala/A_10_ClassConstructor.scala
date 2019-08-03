package com.sev7e0.spark.scala

/**
 * Scala的类构造器实践
 */
object A_10_ClassConstructor {
  def main(args: Array[String]): Unit = {
    //不指定构造器第二个参数,则时使用默认值!
    val d1 = new A_10_ClassConstructor(1)
    val d2 = new A_10_ClassConstructor(1, 2)
    val d3 = new A_10_ClassConstructor(1, 2, 3)

    println(d1.demo())
    println(d2.demo())
    println(d3.demo())
  }
}

/**
 * 主构造函数,默认为 val,可以指定为 var, 可提供默认值,存在默认值得情况下
 * 调用实例化时可以不指定这个参数.
 */
class A_10_ClassConstructor(var a: Int, b: Int = 0) {
  var c = 0
  //私有构造器,类外无法访问 实例对象中也不行
  private[this] var d = 0

  //添加辅助构造函数
  def this(a: Int, b: Int, c: Int) {
    this(a, b)
    this.c = c
  }

  def demo(): Int = {
    a = a + b + c
    //    b = a+b+c 不可用,b 默认为 val 类型的 immutable parameter
    a
  }
}
