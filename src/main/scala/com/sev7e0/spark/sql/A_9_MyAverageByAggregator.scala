package com.sev7e0.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-07 22:46
  **/

case class Employee(name:String, salary:Long)
case class Average(var sum:Long, var count:Long)
object A_9_MyAverageByAggregator extends Aggregator[Employee, Average, Double]{
  override def zero: Average = Average(0L,0L)

  override def reduce(b: Average, a: Employee): Average = {
    b.sum += a.salary
    b.count+=1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.count+=b2.count
    b1.sum+=b2.sum
    b1
  }

  override def finish(reduction: Average): Double = reduction.sum.toDouble/reduction.count

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("MyAverageByAggregator")
      .getOrCreate()
    //隐式转换
    import sparkSession.implicits._
    val dataFrame = sparkSession.read.json("src/main/resources/sparkresource/employees.json").as[Employee]
    dataFrame.show()

    val salary_average = A_9_MyAverageByAggregator.toColumn.name("salary_average")

    val frame = dataFrame.select(salary_average)
    frame.show()
  }
}


