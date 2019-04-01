package com.sev7e0.spark.scala

object Extractor {
  def main(args: Array[String]): Unit = {
    //测试第一种提取器 输出hi comon user lee
    val lee = new CommonUser("lee")
    lee match {
      case CommonUser(name) => println("hi common user "+lee.name)
      case VIPUser(name) => println("hello VIP user "+lee.name)
    }


    /**
      * 多值提取器，从对象的多个属性判断
      */
    val java = new CommonUsers("java",70, 0)
    java match {
      case CommonUsers(name, _, sex) =>
        if (sex == 0){
          println(java.name+" is a man")
        }else{
          println(java.name+" is a woman ")
        }
      case VIPUsers(arg) => println("cant identify")
    }

    /**
      * 布尔提取器
      */
    val scala = new CommonUsersBoo("scala",70, 0)
    scala match {
        //使用@修饰 模式匹配将提取器匹配成功的实例绑定到一个变量上
      case comUser @CommonUsersBoo() => println(comUser.name +" use boolean extractor identity is a man")
      case _ => println("cant identify")
    }


  }

}

/**
  * 第一种自定义提取器的简单实现
  */
trait User{
  def name:String
}
class CommonUser(val name: String) extends User
class VIPUser(val name: String) extends User

/**
  * 提取器主要依靠伴生对象中的unapply方法实现，也可以在其中实现自己的逻辑，
  */
object CommonUser{
  def unapply(arg: CommonUser): Option[String] = Some(arg.name)
}
object VIPUser{
  def unapply(arg: VIPUser): Option[String] = Some(arg.name)
}

/**
  * 多值提取器
  */
trait Users{
  def name: String
  def score: Int
}
class CommonUsers(val name: String,
                  val score: Int,
                  val sex:Int) extends User
class VIPUsers(val name: String,
               val score: Int) extends User

/**
  *
  */
object CommonUsers{
  def unapply(arg: CommonUsers): Option[(String, Int, Int)] = Some((arg.name, arg.score, arg.sex))
}
object VIPUsers{
  def unapply(arg: VIPUsers): Option[(String, Int)] = Some((arg.name, arg.score))
}

/**
  * 布尔提取器
  */
trait UsersBoo{
  def name: String
  def score: Int
}
class CommonUsersBoo(val name: String,
                  val score: Int,
                  val sex:Int) extends User
class VIPUsersBoo(val name: String,
               val score: Int) extends User

object CommonUsersBoo{
  def unapply(arg: CommonUsersBoo): Boolean = arg.sex == 0
}
object VIPUsersBoo{
  def unapply(arg: VIPUsersBoo): Option[(String, Int)] = Some((arg.name, arg.score))
}

