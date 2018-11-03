package com.lijiaqi.spark.core
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOperation_Scala")
    val context = new SparkContext(conf)
  }

  def reduce(context: SparkContext): Unit ={

  }

  def count(context: SparkContext): Unit ={

  }

  def collect(context: SparkContext): Unit ={

  }

  def foreach(context: SparkContext): Unit ={

  }

  def take(context: SparkContext): Unit ={

  }

}














