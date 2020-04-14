package com.sev7e0.wow.core.java;

import com.sev7e0.wow.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;


public class Persist {

    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContextUtil("Persist", "local").getContext();
        //使用rdd持久化，必须在transformation或者textFile加载完成后创建完rdd。直接调用，否则持久化操作不会生效
        JavaRDD<String> stringJavaRDD = context.textFile("src/main/resources/sparkresource/people.txt");

        //Persist this RDD with the default storage level (`MEMORY_ONLY`).
        //cache默认调用的就是persist无参方法，无参默认级别为`MEMORY_ONLY`
        stringJavaRDD.cache();
        //相当于以下调用
//        stringJavaRDD.persist(StorageLevel.MEMORY_ONLY());

        //第一次使用时没有进行缓存
        testCache(stringJavaRDD);

        //第二次基于缓存数据进行计算
        testCache(stringJavaRDD);
        /**
         * 输出：
         *
         * 3
         * 211ms
         * ====================
         * 3
         * 18ms
         * ====================
         **/
    }


    private static void testCache(JavaRDD<String> stringJavaRDD) {
        long currentTimeMillis = System.currentTimeMillis();

        long count = stringJavaRDD.count();

        System.out.println(count);

        long endTime = System.currentTimeMillis();

        System.out.println(endTime - currentTimeMillis + "ms");
        System.out.println("====================");
    }
}
