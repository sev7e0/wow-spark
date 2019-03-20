package com.lijiaqi.spark.scala
import scala.util.Try
import java.net.URL

import scala.io.StdIn

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-20 14:04
  **/
object ErrorHandling {

  def main(args: Array[String]): Unit = {
    /**
      * 异常的抛出与捕获,Try类中底层也是使用了try catch 语法，不过使用了Success和Failure两个子类
      */
    val customer = Customer(15)
    try{
      buyCigarettes(customer)
    }catch {
      case message:UnderAgeException =>println(message.message)
    }

    /**
      * 函数式的异常处理
      */
    triedUrl(StdIn.readLine("URL: "))




  }

  /**
    *
    * @param url url地址
    * @return 返回类型为Try[url]，需要注意得是如果URL在实例化时，如果正确将会返回Success[URL]失败是会返回Failure[URL]
    *         def apply[T](r: => T): Try[T] =
    *           try Success(r)
    *           catch {
    *             case NonFatal(e) => Failure(e)
    *           }
    *
    */
  def triedUrl(url:String):Try[URL] = Try(new URL(url))



  def buyCigarettes(customer: Customer): Cigarettes =
    if (customer.age < 16)
      throw UnderAgeException(s"Customer must be older than 16 but was ${customer.age}")
    else new Cigarettes

}

case class Customer(age :Int)
class Cigarettes
case class UnderAgeException(message: String) extends Exception(message)