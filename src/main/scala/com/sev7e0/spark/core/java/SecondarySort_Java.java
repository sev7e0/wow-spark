package com.sev7e0.spark.core.java;

import com.sev7e0.spark.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 * 1.实现自定义的key，同时该类要实现Ordered借口和Serializable接口，同时要实现大于等于小于等接口方法
 * 2.将文本map成为 key为自定义key，value为每一行文本的rdd
 * 3.使用sortByKey按照自定义的key进行排序
 * 3.再次map，剔除掉自定义的key，只保留文本
 */
public class SecondarySort_Java {
    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContextUtil("SecondarySort_Java", "local").getContext();

        JavaRDD<String> lineRdd = context.textFile("file:///home/sev7e0/DataSets/secondarysort.txt");
        JavaPairRDD<SecondarySortKey, String> pairRDD = lineRdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                SecondarySortKey sortKey = new SecondarySortKey(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
                return new Tuple2<>(sortKey, s);
            }
        });
        JavaRDD<String> javaRDD = pairRDD.sortByKey().map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> secondarySortKeyStringTuple2) throws Exception {
                return secondarySortKeyStringTuple2._2;
            }
        });

        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        context.close();
    }
}
