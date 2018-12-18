package com.lijiaqi.spark.algorithm;

import java.util.Arrays;

/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-14 16:06
 **/

public class AlgorithmUtil {

    public static int[] getRepeatArray(){
        return new int[]{1,5,4,7,3,25,5,5,6,6,6,6,5,5,6,6};
    }

    public static void print(int[] arr){
        //注意Arrays.asList()返回的是一个被包装的List(实质是数组),所有对应的操作最后都会直接操作数组
        //使用时，当传入基本数据类型的数组时，会出现小问题，会把传入的数组整个当作返回的List中的第一个元素
        //这里使用Arrays.stream()
        Arrays.stream(arr).forEach(System.out::print);
        System.out.println();
    }

    public static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    public static void main(String[] args) {
        TimerUtil timerUtil = new TimerUtil();
        timerUtil.getTime();
    }
}
