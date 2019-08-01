package com.sev7e0.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountLocal {

    public static void main(String[] args) {
        //编写Spark应用程序
        //第一步创建SparkConf对象
        //setMaster设置master节点的url
        SparkConf conf = new SparkConf().setAppName("WordCountLocal")
                //指定Master 本地开发时推荐使用standalone模式
                .setMaster("local");
        //创建context，作为程序的入口
        //最重要的对象
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("file:/home/sev7e0/bigdata/spark-2.2.0-bin-hadoop2.7/README.md");

        //将每一行拆分成单个的单词
        //FlatMapFunction包含两个泛型参数,String(输入)/Object*(输出)
        final JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> reduce = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        reduce.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
            }
        });

        context.close();

    }
}
