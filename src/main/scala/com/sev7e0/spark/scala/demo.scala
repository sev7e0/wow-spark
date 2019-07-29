package com.sev7e0.spark.scala


class demo(b: String) {
  var a = b
  def this(c: Int) =
    this(String.valueOf(c))

  println(a)
}


object a{
  def main(args: Array[String]): Unit = {
    new demo("ddd")
  }
}