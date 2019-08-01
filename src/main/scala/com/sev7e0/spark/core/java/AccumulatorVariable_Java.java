package com.sev7e0.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * spark中的全局累加器，用实现sum count的操作
 */
public class AccumulatorVariable_Java {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName(AccumulatorVariable_Java.class.getName());

        //获取 SparkContext
        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //获取累加器
        final LongAccumulator accumulator = jsc.sc().longAccumulator();

        accumulator.setValue(0);
        //Spark2.0中不在推荐使用Accumulator
        //final Accumulator<Integer> accumulator = context.intAccumulator(5);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //分发本地scala集合以形成RDD,将 list 转换成为 rdd
        JavaRDD<Integer> rdd = jsc.parallelize(list);

        rdd.foreach(integer -> accumulator.add(Long.valueOf(integer)));

        System.out.println(accumulator.value());
    }
}
