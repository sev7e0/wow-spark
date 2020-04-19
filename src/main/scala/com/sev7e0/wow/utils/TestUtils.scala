package com.sev7e0.wow.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestUtils extends FunSuite with BeforeAndAfter {
	
	var conf: SparkConf = _
	var context: SparkContext = _
	
	before {
		conf = new SparkConf()
			.setMaster("local")
			.setAppName("wow-spark")
		context = new SparkContext(conf)
		context.setLogLevel("WARN")
	}
	
	after(println("have a good time"))
}
