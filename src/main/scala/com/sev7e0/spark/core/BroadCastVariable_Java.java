package com.sev7e0.spark.core;

import com.sev7e0.spark.JavaSparkContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Broadcast  广播变量
 */
@Slf4j
public class BroadCastVariable_Java {

    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContextUtil("BroadCastVariable_Java", "local").getContext();
        List<Integer> list = Arrays.asList(1, 2, 5, 8, 9, 6);

        final int factor = 5;
        //设置factor为共享变量
        final Broadcast<Integer> broadcast = context.broadcast(factor);

        JavaRDD<Integer> listRdd = context.parallelize(list);

        JavaRDD<Integer> numRdd = listRdd.map((Function<Integer, Integer>) integer -> {
            //通过value（）方法获取broadcast中设置的广播变量
            return integer * broadcast.value();
        });
        //遍历输出
        numRdd.foreach(integer -> log.info("计算后的值为 {}",integer.toString()));

    }
}
