package com.lijiaqi.spark.algorithm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.LongStream;

/**
 * @description:
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-18 16:57
 **/

public class TimerUtil {

    private static TreeMap<String, Long> getTimeTable(){
        TreeMap<String, Long> treeMap = new TreeMap<>();
        //获取到当前堆栈的信息,找到当前调用该方法的相关信息
        String className = Thread.currentThread().getStackTrace()[3].getClassName();
        System.out.println("current class name is:["+className+"]");
        try {
            //通过反射获取当被添加注解的方法并执行
            Class name = Class.forName(className);
            Object instance = name.newInstance();
            Method[] declaredMethods = name.getDeclaredMethods();
            Arrays.stream(declaredMethods).forEach(method->{
                if (method.isAnnotationPresent(Timer.class)){
                    method.setAccessible(true);
                    LocalDateTime start = LocalDateTime.now();
                    try {
                        method.invoke(instance);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    LocalDateTime end = LocalDateTime.now();
                    Duration duration = Duration.between(start, end);
                    treeMap.put(method.getName(), duration.toMillis());
                }
            });
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return treeMap;
    }

    static void printChart(){
        //value升序排列
        Map<String, Long> timeTable = sortByValue(getTimeTable());
        long max = timeTable.values().iterator().next();
        timeTable.forEach((String a, Long b) -> {
            long percent = (b * 100)/ max;
            LongStream.range(0, percent).mapToObj(c -> "=").forEach(System.out::print);
            System.out.println(" method:"+a+"  time:"+b+"  relative efficiency:"+percent+"%");
        });
    }
    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        // desc order
        list.sort((o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static void main(String[] args) {
        TimerUtil timerUtil = new TimerUtil();
        timerUtil.printChart();
    }
}
