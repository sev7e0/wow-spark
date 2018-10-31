package com.lijiaqi.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 字符统计java
 */
public class LineCountLocal_Java implements AutoCloseable{

    private static JavaSparkContext context;

    static {
        SparkConf sparkConf = new SparkConf().setAppName("LineCountLocal").setMaster("local");
        context = new JavaSparkContext(sparkConf);
    }
    public static void main(String[] args) {
        JavaRDD<String> javaRDD = context.textFile("file:/home/sev7e0/bigdata/spark-2.2.0-bin-hadoop2.7/README.md",5);
        JavaRDD<Integer> integerJavaRDD = javaRDD.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) {
                return s.length();
            }
        });

        Integer res = integerJavaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });
        System.out.println(res);

    }

    @Override
    public void close() {
        context.close();
    }
}
