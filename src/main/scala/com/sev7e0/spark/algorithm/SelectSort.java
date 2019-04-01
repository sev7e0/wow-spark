package com.sev7e0.spark.algorithm;


/**
 * @description: TODO
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-27 16:15
 **/

public class SelectSort {

    public static void main(String[] args) {
        TimerUtil.printChart();
    }

    @Timer
    public void selectSort(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        int lenth = arr.length;
        int min = 0;
        //位置
        for (int i = 0; i <lenth-1;i++){
            //找值
            min = i;
            for (int j =i+1;j <lenth;j++){
                if (arr[min]>arr[j]){
                    min = j;
                }
            }
            if (min != i)
                AlgorithmUtil.swap(arr,min,i);
        }
        AlgorithmUtil.print(arr);
    }
}
