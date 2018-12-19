package com.lijiaqi.spark.algorithm;

/**
 * @description:
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-18 16:17
 **/

public class ShellSort {
    public static void main(String[] args) {
        shellSort(AlgorithmUtil.getRepeatArray());
    }
    private static void shellSort(int[] arr){
        int j;
        int length = arr.length;
        //计算每次的步长,默认每次取中
        for (int gap = length/2; gap>0; gap/=2){
            for (int i = 0; i+gap<length; i++){
                int temp = arr[i];
                for (j=i+gap; j<length && temp>arr[j];j+=gap){
                    arr[i] = arr[j];
                }
                arr[j-gap] = temp;
            }
        }
        AlgorithmUtil.print(arr);
    }
}
