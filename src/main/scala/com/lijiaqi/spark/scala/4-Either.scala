package com.lijiaqi.spark.scala


import java.net.URL

import scala.io.Source

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-22 16:29
  **/
object UseEither {
  def main(args: Array[String]): Unit = {
    val url = new URL("http://www.google.com")
    getContent(url) match {
      case Left(message) => println(message)
      case Right(source) => source.getLines().foreach(println)
    }

    println(averageLineCount(url, url))

  }

  def averageLineCount(url1:URL, url2:URL): Unit ={
    for {
      content <- getContent(url1).right
      content1 <- getContent(url2).right
    }yield (content.getLines().size + content1.getLines().size)/2
  }

  /**
    *
    * @param url 请求地址
    * @return
    */
  def getContent(url:URL): Either[String, Source] ={
    if (url.getHost.contains("google")){
      Left("request url is blocked")
    }else{
      Right(Source.fromURL(url))
    }
  }
}
