package com.sev7e0.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @program: spark-learn
 * @description: 对文件内的数字, 取最大的前三个,对班级分组取top n
 * @author: Lijiaqi
 * @create: 2018-11-16 00:33
 **/
public class TopN_Java {
    public static void main(String[] args) {
        SparkConf sparkconf = new SparkConf();
        SparkConf conf = sparkconf.setAppName("topn").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> textFile = context.textFile("/Users/sev7e0/dataset/topn.txt");

        //第一件事就是将他映射成为一个k v对,然后根据他的k进行排序
        JavaPairRDD<Integer, String> pairRDD = textFile.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s),s);
            }
        });

        //使用sortByKey的时候传递false参数表示按照降序排列
        JavaPairRDD<Integer, String> pairRDD1 = pairRDD.sortByKey(false);

        //讲排好序的rdd取出其中的value值
        JavaRDD<Integer> rdd = pairRDD1.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2._1;
            }
        });

        List<Integer> take = rdd.take(3);

        take.stream().forEach(integer -> System.out.println(integer.toString()));


        context.close();
    }
}
