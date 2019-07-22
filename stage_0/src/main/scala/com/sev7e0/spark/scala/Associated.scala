package com.sev7e0.spark.scala

class Associated {

  val animal = 4
  //类可以直接访问伴生对象中的元素
  val b = Associated.buy
}

object Associated {
  //伴生对象不能直接访问类中的元素
//  val a = Associated.animal
  val a = new Associated().animal
  val buy = 2
}
