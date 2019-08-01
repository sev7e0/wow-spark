package com.sev7e0.spark.core.java;

import com.sev7e0.spark.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class Persist {

    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContextUtil("Persist", "local").getContext();
        //使用rdd持久化，必须在transformation或者textFile加载完成后创建完rdd。直接调用，否则持久化操作不会生效
        JavaRDD<String> stringJavaRDD = context.textFile("src/main/resources/sparkresource/people.txt")
                .persist(StorageLevel.MEMORY_ONLY());

        long currentTimeMillis = System.currentTimeMillis();

        long count = stringJavaRDD.count();

        System.out.println(count);

        long endTime = System.currentTimeMillis();

        System.out.println(endTime-currentTimeMillis+"ms");

        long startTime = System.currentTimeMillis();

        long count2 = stringJavaRDD.count();

        System.out.println(count2);

        long endTime2 = System.currentTimeMillis();

        System.out.println(endTime2-startTime+"ms");


    }
}
