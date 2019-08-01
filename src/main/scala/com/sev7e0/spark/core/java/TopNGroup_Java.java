package com.sev7e0.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @program: spark-learn
 * @description: 分组取topn
 * @author: Lijiaqi
 * @create: 2018-11-16 01:18
 **/
public class TopNGroup_Java {

    public static void main(String[] args) {

        SparkConf sprakConf = new SparkConf();
        SparkConf sparkConf = sprakConf.setAppName("TopNGroup_Java").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = context.textFile("/Users/sev7e0/dataset/topn-group.txt");

        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapToPair((PairFunction<String, String, Integer>) s -> {
            String[] strings = s.split(" ");
            return new Tuple2<String, Integer>(strings[0], Integer.valueOf(strings[1]));
        });

        JavaPairRDD<String, Iterable<Integer>> groupRdd = pairRDD.groupByKey();

        groupRdd.foreach((VoidFunction<Tuple2<String, Iterable<Integer>>>)stringIterableTuple2 -> {
                System.out.println(stringIterableTuple2._1);
                stringIterableTuple2._2.forEach(score-> System.out.println(score));
        });

        JavaRDD<Tuple2<String, Iterable<Integer>>> map = groupRdd.map((Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, Iterable<Integer>>>) stringIterableTuple2 -> {
            Integer[] integers = new Integer[3];
            stringIterableTuple2._2.forEach(integer -> {

                for (int i = 0; i < 3; i++) {
                    if (integers[i] == null) {
                        integers[i] = integer;
                        break;
                    }
                    if (integer > integers[i]) {
                        for (int j = 2; j < i; j--) {
                            integers[j] = integers[j - 1];
                        }
                        integers[i] = integer;
                        break;
                    }
                }
            });
            return new Tuple2<>(stringIterableTuple2._1, Arrays.asList(integers));
        });





        map.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1);
                stringIterableTuple2._2.forEach(score->{
                    System.out.println(score);
                });
            }
        });


    }

}
