package com.sev7e0.spark.scala

import java.io.FileNotFoundException

import scala.util.{Failure, Success, Try}
import java.net.{MalformedURLException, URL}


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
//    triedUrl(StdIn.readLine("URL: "))

    /**
      * 和option一样支持高阶函数操作
      */
    triedUrl("http://www.baidu.com").map(_.getPort).foreach(println)

    getUrlContent("http://www.baidu.com").foreach(println)

    /**
      * 在返回Try实例时你可以使用模式匹配
      */
    getUrlContent("http://www.baidu.com") match {
      case Failure(exception) => println(exception.getMessage)
      case Success(value) => value.foreach(println)
    }

    /**
      * 执行失败时如果想执行其他逻辑，这里我们可以使用recover，他接受定义一个偏函数，
      * 但只有在返回Failure实例时才会执行，Success实例则不会使用偏函数，直接返回结果
      */
    val result = getUrlContent("httP；") recover {
      case e: FileNotFoundException => Iterator("Requested page does not exist")
      case e: MalformedURLException => Iterator("Please make sure to enter a valid URL")
      case _ => Iterator("An unexpected error has occurred. We are so sorry!")
    }
    result.foreach(println)
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
  def triedUrl(url:String):Try[URL] = Try(new URL(url)) orElse Try(new URL("www.baidu.com"))


  import scala.io.Source
  def getUrlContent(url:String): Try[Iterator[String]] ={
    val a = for {
      url <- triedUrl(url)
      connection <- Try(url.openConnection())
      is <- Try(connection.getInputStream)
      content = Source.fromInputStream(is)
//      content = Source.fromURL(url)
    } yield content.getLines()
    /**
      * For each iteration of your for loop, yield generates a value which will be remembered.
      * It's like the for loop has a buffer you can't see, and for each iteration of your for loop, another item is added to that buffer.
      * When your for loop finishes running, it will return this collection of all the yielded values.
      * The type of the collection that is returned is the same type that you were iterating over, so a Map yields a Map, a List yields a List, and so on.
      *
      * Also, note that the initial collection is not changed; the for/yield construct creates a new collection according to the algorithm you specify.
      */
    //yield关键字在使用循环时会针对你指定的属性，使用一个buffer生成对应的集合（循环什么集合就会产生什么集合），不会对原集合产生影响是一个全新的
    a
  }

  def buyCigarettes(customer: Customer): Cigarettes =
    if (customer.age < 16)
      throw UnderAgeException(s"Customer must be older than 16 but was ${customer.age}")
    else new Cigarettes

}

case class Customer(age :Int)
class Cigarettes
case class UnderAgeException(message: String) extends Exception(message)