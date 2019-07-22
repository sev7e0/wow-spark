package com.sev7e0.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-03 23:57
  **/
object A_8_MyAverage extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("inputColumn",LongType)::Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)= 0l
    buffer(1)= 0l
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1

    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("A_8_MyAverage")
      .master("local")
      .getOrCreate()
    sparkSession.udf.register("A_8_MyAverage",A_8_MyAverage)

    val dataFrame = sparkSession.read.json("src/main/resources/sparkresource/employees.json")
    dataFrame.createOrReplaceTempView("employees")

    val result = sparkSession.sql("select A_8_MyAverage(salary) as average_salary from employees")
    result.show()
  }
}

