package com.lijiaqi.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVaribale_Java {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("AccumulatorVariable_Scala");
        final JavaSparkContext context = new JavaSparkContext(sparkConf);

        final LongAccumulator accumulator = context.sc().longAccumulator();
        accumulator.setValue(0);
        //Spark2.0中不在推荐使用Accumulator
//        final Accumulator<Integer> accumulator = context.intAccumulator(5);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        JavaRDD<Integer> rdd = context.<Integer>parallelize(list);

//        rdd.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) {
//                accumulator.add(Long.valueOf(integer));
//            }
//        });
        //将上边代码改写为lambda风格
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(Long.valueOf(integer));
            }
        });
        System.out.println(accumulator.value());
    }
}
