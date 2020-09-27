package com.sev7e0.wow.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.HashMap;
import java.util.Objects;

/**
 * Title:  SparkStreamingWriteToEs.java
 * description: spark写入es
 *
 * @author sev7e0
 * @version 1.0
 * @since 2020-06-21 15:33
 **/

public class SparkStreamingReadExcelWriteToEs {


	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			.setAppName(SparkStreamingReadExcelWriteToEs.class.getName())
			.setMaster("local[*]");

		sparkConf.set(ConfigurationOptions.ES_NODES, LocalConfiguration.ES_NODES);
		sparkConf.set(ConfigurationOptions.ES_PORT, LocalConfiguration.ES_PORT);
		sparkConf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, LocalConfiguration.ES_NODES_WAN_ONLY);
		sparkConf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, LocalConfiguration.ES_INDEX_AUTO_CREATE);
		sparkConf.set(ConfigurationOptions.ES_NODES_DISCOVERY, LocalConfiguration.ES_NODES_DISCOVERY);
		sparkConf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, LocalConfiguration.ES_NET_HTTP_AUTH_USER);
		sparkConf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, LocalConfiguration.ES_NET_HTTP_AUTH_PASS);



		final SparkSession session = SparkSession.builder().config(sparkConf).appName(SparkStreamingReadExcelWriteToEs.class.getName())
			.master("local")
			.getOrCreate();

		final Dataset<Row> load = session.read()
			.format("com.crealytics.spark.excel")
			.option("header", "true") // Required
			.option("treatEmptyValuesAsNulls", "false") // Optional, default: true
			.option("inferSchema", "false") // Optional, default: false
			.option("addColorColumns", "true") // Optional, default: false
			.option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
			.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
			.option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
			.load("/Users/test.xlsx");

		final JavaRDD<String> jsonRdd = load.toJavaRDD().map(row -> {
			if (Objects.nonNull(row.get(1))) {
				final HashMap<String, Object> map = new HashMap<>();
				map.put("type", row.get(0));
				map.put("fullName", row.get(1));
				map.put("simpleName", row.get(2));
				final Gson gson = new Gson();
				return gson.toJson(map);
			}
			return null;
		}).filter(Objects::nonNull);

		JavaEsSpark.saveJsonToEs(jsonRdd, "fullname/name");

		session.stop();
	}

}
