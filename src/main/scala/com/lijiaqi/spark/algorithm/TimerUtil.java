package com.lijiaqi.spark.algorithm;

import javax.ws.rs.GET;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-18 16:57
 **/

public class TimerUtil {

    public void getTime(){
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        System.out.println("current class name is:["+className+"]");
        try {
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
                    System.out.println("method :"+method.getName()+" time:"+duration.toMillis());
                }
            });
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TimerUtil timerUtil = new TimerUtil();
        timerUtil.getTime();
    }
}
