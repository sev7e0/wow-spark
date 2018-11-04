package com.lijiaqi.spark.core;

import com.lijiaqi.spark.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Broadcast   实现共享变量
 */
public class BroadCastVariable_Java {

    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContextUtil("BroadCastVariable_Java", "local").getContext();
        List<Integer> list = Arrays.asList(1, 2, 5, 8, 9, 6);
        final int factor = 5;
        //设置factor为共享变量
        final Broadcast<Integer> broadcast = context.broadcast(factor);

        JavaRDD<Integer> listRdd = context.parallelize(list);

        JavaRDD<Integer> numRdd = listRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                //通过value（）方法获取broadcast中设置的共享变量
                return integer * broadcast.value();
            }
        });

        numRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }
}
