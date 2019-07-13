package com.sev7e0.spark.kafka;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class st_56980436 {

    public static void main( String[] args ) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("App").getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "randomCount")
                .option("startingOffsets", "earliest")
                .load();
        df.printSchema();
        StreamingQuery query = df.writeStream().trigger(Trigger.ProcessingTime(1000)).format("console").start();
        AppInfoParser.getVersion();
        try {
            query.awaitTermination();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
