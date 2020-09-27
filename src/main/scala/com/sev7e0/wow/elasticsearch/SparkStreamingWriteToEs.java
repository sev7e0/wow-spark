package com.sev7e0.wow.elasticsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * Title:  SparkStreamingWriteToEs.java
 * description: spark写入es
 *
 * @author sev7e0
 * @version 1.0
 * @since 2020-06-21 15:33
 **/

public class SparkStreamingWriteToEs {


	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			.setAppName(SparkStreamingWriteToEs.class.getName())
			.setMaster("local[*]");

		sparkConf.set(ConfigurationOptions.ES_NODES, LocalConfiguration.ES_NODES);
		sparkConf.set(ConfigurationOptions.ES_PORT, LocalConfiguration.ES_PORT);
		sparkConf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, LocalConfiguration.ES_NODES_WAN_ONLY);
		sparkConf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, LocalConfiguration.ES_INDEX_AUTO_CREATE);
		sparkConf.set(ConfigurationOptions.ES_NODES_DISCOVERY, LocalConfiguration.ES_NODES_DISCOVERY);
		sparkConf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, LocalConfiguration.ES_NET_HTTP_AUTH_USER);
		sparkConf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, LocalConfiguration.ES_NET_HTTP_AUTH_PASS);


		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> jsonRdd = context.textFile("src/main/resources/sparkresource/device.json", 5);

		JavaEsSpark.saveJsonToEs(jsonRdd, "/device");


	}

}
