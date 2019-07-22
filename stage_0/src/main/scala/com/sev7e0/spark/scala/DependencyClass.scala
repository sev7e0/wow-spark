package com.sev7e0.spark.scala

object DependencyClass {
  def main(args: Array[String]): Unit = {

    /**
      * 第一种方式实现
      */
    val hulk = new Franchise("hulk")
    val iron = new Franchise("iron man")

    val soldier = Franchise.Character("winter soldier", hulk)
    val loki = Franchise.Character("loki", hulk)
    //当前只有在运行时才能确定是否为同一个类型下的
    println(hulk.createFanFiction(soldier, loki))
    //(Character(winter soldier,com.sev7e0.spark.scala.Franchise@5f282abb),Character(loki,com.sev7e0.spark.scala.Franchise@5f282abb))

    /**
      * 第二种方式实现
      */
    val captain = new FranchiseNew("captain")
    val red = new FranchiseNew("red hulk")

    val pool = captain.CharacterNew("dead pool")
    val thor = captain.CharacterNew("thor")

    val window = red.CharacterNew("black window")

//    captain.createFanFiction(pool, window)  编译不通过，Type Mismatch
    println(captain.createFanFiction(pool, thor))
    //(CharacterNew(dead pool),CharacterNew(thor))
  }



}

object Franchise{
  case class Character(name: String, franchise: Franchise)
}

class Franchise(name: String){
  import Franchise.Character
  def createFanFiction(
                      ch1: Character,
                      ch2: Character
                      ):(Character, Character) = {
    //这里要求两个对象只能由一个Franchise实例创建，若不同则直接抛出异常。但是缺点是该异常只能在运行时被发现。
    require(ch1.franchise == ch2.franchise)
    (ch1, ch2)
  }
}

/**
  * 优化后的解决方法，在编译器就能确定错误。
  */
class FranchiseNew(name: String){

  //将CharacterNew嵌套到了FranchiseNew中，这样只能依赖特定的实例
  case class CharacterNew(name: String)
  def createFanFiction(
                        ch1: CharacterNew,
                        ch2: CharacterNew
                      ):(CharacterNew, CharacterNew) = {
    (ch1, ch2)
  }

  /**
    *  参数的类型依赖于传递给该方法的 Franchise 实例。 不过请注意：被依赖的实例只能在一个  单独   的参数列表里。
    */
  def createFanFiction(f: FranchiseNew)(lovestruck: f.CharacterNew, objectOfDesire: f.CharacterNew) =
    (lovestruck, objectOfDesire)
}