package com.lijiaqi.spark.algorithm;

import java.util.Arrays;

/**
 * @description: TODO
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-19 17:59
 **/

/**
 * 中心思想:将待排序列分为有序序列和无序序列,从无序的头部开始两两比较,根据比较的大小进行前后交换,一趟排序后无序序列尾部值(最大的值)放入有序序列.
 *         开始第二次无序队列遍历,和上一步相同原理,重复上面的步骤，直到没有任何一对数字需要比较，则序列最终有序。
 * 相关介绍: https://www.kancloud.cn/maliming/leetcode/737441
 */

public class BubbleSort {
    public static void main(String[] args) {
        TimerUtil.printChart();
    }

    @Timer
    public void bubbleSort(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        int length = arr.length;
        Arrays.stream(arr).forEach(a->{
            for (int i =0; i<length-1;i++){
                if (arr[i]>arr[i+1]){
                    AlgorithmUtil.swap(arr,i,i+1);
                }
            }
        });
        AlgorithmUtil.print(arr);
    }

    @Timer
    public void bubbleSort2(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        int lenth = arr.length;
        for (int i =0; i<lenth; i++){
            for (int j=i+1; j<lenth; j++){
                if (arr[i]>arr[j]){
                    AlgorithmUtil.swap(arr, i, j);
                }
            }
        }
        AlgorithmUtil.print(arr);
    }
}
