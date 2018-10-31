package com.lijiaqi.spark.core;

import com.lijiaqi.spark.JavaSparkContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每行出现的次数java版本
 */
public class LineCount_Java implements AutoCloseable {

    private static JavaSparkContext context = new JavaSparkContextUtil("LineCountLocal","local").getContext();

    public static void main(String[] args) {
        JavaRDD<String> rdd = context.textFile("file:///home/sev7e0/DataSets/linecount.txt");
        //对每一行产生的rdd执行mapToPair算子,将每一行映射成为(line,1)数据结构,
        //然后才能统计每一行出现的次数
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //对pairrdd执行reduceByKey
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //通过key进行排序,在返回新的有序rdd
        JavaPairRDD<String, Integer> sortRdd = reduceByKey.sortByKey();
        //执行action操作
        sortRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2+"times");
            }
        });
    }

    @Override
    public void close() throws Exception {
        context.close();
    }
}
