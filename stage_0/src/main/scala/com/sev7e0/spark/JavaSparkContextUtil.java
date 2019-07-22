package com.sev7e0.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 获取sparkcontext工具类
 */
public class JavaSparkContextUtil {

    private static JavaSparkContext context;

    public JavaSparkContextUtil(String appName,String master) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        if (master != null)
            sparkConf.setMaster(master);
        context = new JavaSparkContext(sparkConf);
    }

    public JavaSparkContextUtil(String appName) {
        new JavaSparkContextUtil(appName,null);
    }

    public static JavaSparkContext getContext(){
        return context;
    }

}
