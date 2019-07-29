package com.sev7e0.spark.scala


object A_9_InnerMethod {

  def main(args: Array[String]): Unit = {
    outMethod

  }


  private [scala]
  def outMethod(): Unit ={

    innerMethod()

    def innerMethod(): Unit ={
      println("this is inner method!")
    }

    println("outer method!")
  }
}
