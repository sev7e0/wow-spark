package com.sev7e0.spark.core.java;

import com.sev7e0.spark.JavaSparkContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ActionOperation相关方法,Action 会触发 job 的执行
 */
public class ActionOperation_Java {
    public static void main(String[] args) {
        //统一使用JavaSparkContextUtil获取context对象
        JavaSparkContext context = new JavaSparkContextUtil("ActionOperation_Java", "local").getContext();

        reduce(context);
        take(context);
        collect(context);
        count(context);
    }

    /**
     * 使用指定的二元运算符减少rdd 元素
     * @param context
     */
    private static void reduce(JavaSparkContext context) {
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Integer reduce = context.parallelize(integerList)
                .reduce((integer, integer2) -> integer * integer2);
        System.out.println(reduce);
    }

    /**
     * 返回一个包含了所有 rdd 元素的数组。
     *
     * 不推荐使用，因为可能会造成大量的网络io
     * 只有当结果数组很小时才应该使用此方法，因为所有数据都加载到驱动程序的内存中。
     *
     * @param context
     */
    private static void collect(JavaSparkContext context) {
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Integer> collect = context.parallelize(integerList)
                .map(integer -> integer * 2)
                .collect();

        for (Integer integer : collect) {
            System.out.println(integer);
        }
    }

    /**
     * 使用getIteratorSize，对当前0 to 所有的 rdd 进行长度统计，返回一个 ArrayList
     * 对 list 使用 sum 求出所有数量。
     *
     * @param context
     */
    private static void count(JavaSparkContext context) {
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        long count = context.parallelize(integerList)
                .count();
        System.out.println(count);
    }

    /**
     * 与collect相似，只不过是从远程拿取rdd中的前n个元素
     *
     * 只有当结果数组很小时才应该使用此方法，因为所有数据都加载到驱动程序的内存中。
     * @param context
     */
    private static void take(JavaSparkContext context) {
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Integer> take = context.parallelize(integerList)
                .take(5);
        for (Integer integer : take) {
            System.out.println(integer);
        }
    }


}
