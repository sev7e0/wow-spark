package com.sev7e0.spark.scala


object A_9_InnerMethod {

  def main(args: Array[String]): Unit = {
    outMethod
  }

  /**
   * Scala 支持在方法中定义私有方法,并且改内部方法在方法被调用时最先执行
   *
   * 以下方法输出为:
   * this is inner method!
   * first print!
   * outer method!
   */

  private[scala]
  def outMethod(): Unit = {

    innerMethod()

    println("first print!")

    def innerMethod(): Unit = {
      println("this is inner method!")
    }

    println("outer method!")
  }
}
