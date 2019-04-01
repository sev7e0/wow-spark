package com.sev7e0.spark.scala

import java.net.URL

import scala.io.Source


object UseEither {
  def main(args: Array[String]): Unit = {
    val url = new URL("http://www.google.com")
    getContent(url) match {
      case Left(message) => println(message)
      case Right(source) => source.getLines().foreach(println)
    }
    val url1 = new URL("http://www.baidu.com")
    println(averageLineCount(url1, url1))

    println(averageLineCount1(url1, url1))

  }


  def averageLineCount1(url1:URL, url2:URL):Int = {
    var product = 0
     getContent(url1).right.map(a =>
       getContent(url2).right.map(b =>
         product = (a.getLines().size + b.getLines().size) >> 1
       )
     )
    product
  }


  def averageLineCount(url1:URL, url2:URL): Either[String, Int] ={
    val a = for {
      content <- getContent(url1).right
      content1 <- getContent(url2).right
    }yield (content.getLines().size + content1.getLines().size)/2
    a
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
