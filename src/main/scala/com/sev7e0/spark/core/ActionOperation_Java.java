package com.sev7e0.spark.core;

import com.sev7e0.spark.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * ActionOperation相关方法
 */
public class ActionOperation_Java {
    public static void main(String[] args) {
        //统一使用JavaSparkContextUtil获取context对象
        JavaSparkContext context = new JavaSparkContextUtil("ActionOperation_Java", "local").getContext();

//        reduce(context);
//        take(context);
//        collect(context);
        count(context);
    }

    //reduce操作
    private static void reduce(JavaSparkContext context){
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Integer reduce = context.parallelize(integerList)
                .reduce(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer * integer2;
                    }
                });
        System.out.println(reduce);
    }

    //从远程获取到rdd，拉去到本地
    //不推荐使用，因为可能会造成大量的网络io
    private static void collect(JavaSparkContext context ){
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Integer> collect = context.parallelize(integerList)
                .map(new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer) throws Exception {
                        return integer*2;
                    }
                })
                .collect();

        for (Integer integer : collect){
            System.out.println(integer);
        }
    }

    //统计rdd中含有多少个元素
    private static void count(JavaSparkContext context){
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        long count = context.parallelize(integerList)
                .count();
        System.out.println(count);
    }

    //与collect相似，只不过是从远程拿取rdd中的前n个元素
    private static void take(JavaSparkContext context){
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Integer> take = context.parallelize(integerList)
                .take(5);
        for (Integer integer : take){
            System.out.println(integer);
        }
    }



}
