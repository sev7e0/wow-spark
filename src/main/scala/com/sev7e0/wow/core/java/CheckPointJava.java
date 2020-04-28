package com.sev7e0.wow.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Title:  CheckPointJava.java
 * description: TODO
 *
 * @author sev7e0
 * @version 1.0
 * @since 2020-04-28 17:27
 **/

public class CheckPointJava {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("CheckPoint test").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		context.setCheckpointDir("target/checkpoint/");
		JavaRDD<String> lineRDD = context.textFile("src/main/resources/sparkresource/access.log", 5);
		lineRDD.checkpoint();
		lineRDD.saveAsTextFile("target/result/");
		context.stop();
		//f53129a7-c29a-4250-a477-c02abd042383
		//	 rdd-3
		//		.part-00000.crc
		//		.part-00001.crc
		//		.part-00002.crc
		//		.part-00003.crc
		//		.part-00004.crc
		//		part-00000
		//		part-00001
		//		part-00002
		//		part-00003
		//		part-00004
	}

}
