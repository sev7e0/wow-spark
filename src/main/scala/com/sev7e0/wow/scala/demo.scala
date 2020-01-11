package com.sev7e0.wow.scala

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.{immutable, mutable}


class demo(b: String) {
  var a = b
  def this(c: Int) =
    this(String.valueOf(c))

  println(a)
}


object a{
  def main(args: Array[String]): Unit = {
    new demo("ddd")
    val options:JDBCOptions = new JDBCOptions(Map("url" -> "JDBCUrl", "dbtable" -> "foo"))

    val connection = JdbcUtils.createConnectionFactory(options)
  }
}