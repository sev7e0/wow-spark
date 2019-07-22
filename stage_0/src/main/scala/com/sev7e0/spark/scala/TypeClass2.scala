package com.sev7e0.spark.scala

object TypeClass2 {
  private val grass = new Grass

  private val fish = new Fish

  def main(args: Array[String]): Unit = {
    val cow = new Cow
    cow.eat(grass)

//    cow.eat(fish)  编译不会通过
  }
}

class Fish extends Food

class Grass extends Food

class Cow extends Animal {
  override type SuitFood = Grass

  override def eat(food: SuitFood): Unit = {
    println(s"food.getClass.getName = ${food.getClass.getName}")
  }

}


class Food

abstract class Animal {
  //限制使用的类型等级
  type SuitFood <: Food

  def eat(food: SuitFood)
}