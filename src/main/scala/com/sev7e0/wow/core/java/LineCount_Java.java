package com.sev7e0.wow.core.java;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sev7e0.wow.JavaSparkContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * 统计每行出现的次数java版本
 */
@Slf4j
public class LineCount_Java implements AutoCloseable {

    private static JavaSparkContext context = new JavaSparkContextUtil("LineCountLocal", "local").getContext();

    public static void main(String[] args) {
        JavaRDD<String> rdd = context.textFile("json.json");

        JavaRDD<DataX> dataX = rdd.map(s -> JSON.parseObject(s, DataX.class));
        /**
         * {
         *     "uid": "83323323",
         *     "classId": "597127",
         *     "renderFrameRateList": [
         *         0,
         *         0,
         *         0,
         *         0,
         *         0,
         *         0,
         *         0
         *     ]
         * }
         */
        JavaRDD<DataX> kdSum = dataX.filter(d -> {
            List<Integer> frameRateList = d.getRenderFrameRateList();
            int n = 0;
            for (Integer num : frameRateList) {
                if (num < 3) {
                    n++;
                }
            }
            return n >= 3;
        });

        long count = kdSum.count();
        long uCount = dataX.count();





        //对每一行产生的rdd执行mapToPair算子,将每一行映射成为(line,1)数据结构,
        //然后才能统计每一行出现的次数
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(s -> new Tuple2<>(s, 1));
        //对pairrdd执行reduceByKey
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey((integer, integer2) -> integer + integer2);
        //通过key进行排序,在返回新的有序rdd
        JavaPairRDD<String, Integer> sortRdd = reduceByKey.sortByKey();
        //执行action操作
        sortRdd.foreach(stringIntegerTuple2
                -> System.out.println(stringIntegerTuple2._1 + ":" + stringIntegerTuple2._2 + "times"));
    }

    @Override
    public void close() {
        context.close();
    }
}


class DataX{
    String uid;
    String classId;
    List<Integer> renderFrameRateList;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getClassId() {
        return classId;
    }

    public void setClassId(String classId) {
        this.classId = classId;
    }

    public List<Integer> getRenderFrameRateList() {
        return renderFrameRateList;
    }

    public void setRenderFrameRateList(List<Integer> renderFrameRateList) {
        this.renderFrameRateList = renderFrameRateList;
    }
}