package com.sev7e0.wow.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @program: wow-spark
 * @description:
 * @author: sev7e0
 * @create: 2018-11-16 00:33
 **/
public class TopN_Java {
    public static void main(String[] args) {
        SparkConf sparkconf = new SparkConf();
        SparkConf conf = sparkconf.setAppName("topn").setMaster("local[1]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> textFile = context.textFile("src/main/resources/sparkresource/access.log");
        JavaRDD<String> filter = textFile.filter(line -> {
            String[] s = line.split("  ");
            return s.length>10;
        });

        JavaPairRDD<String, Integer> pairRDD = filter.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) {
                String[] s = line.split("  ");
                return new Tuple2<>(s[0], 1);
            }
        });

        //相同 key进行聚合
        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //构建新的pair rdd 方便根据key（count值）排序
        JavaPairRDD<Integer, Tuple2<String, Integer>> tuple2JavaRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2);
            }
        });
        JavaPairRDD<Integer, Tuple2<String, Integer>> sortByKeyRDD = tuple2JavaRDD.sortByKey(false);
        List<Tuple2<Integer, Tuple2<String, Integer>>> take = sortByKeyRDD.take(5);

        take.forEach(integer -> System.out.println(integer._2));

        context.close();
        //(101.226.68.137,972)
        //(163.177.71.12,972)
        //(183.195.232.138,971)
        //(111.192.165.229,334)
        //(114.252.89.91,294)
    }
}
