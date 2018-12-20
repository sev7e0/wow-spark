package com.lijiaqi.spark.algorithm;

import java.util.Arrays;

/**
 * @description: TODO
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-19 17:59
 **/

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
