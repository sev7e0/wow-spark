package com.lijiaqi.spark.algorithm;

/**
 * @description: TODO
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-26 14:35
 **/

public class MergeSort {

    public static void main(String[] args) {
        TimerUtil.printChart();
    }

    @Timer
    public void initMergeSort(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        int[] temp = new int[arr.length];
        mergeSort(arr, temp, 0, arr.length-1);
        AlgorithmUtil.print(arr);
    }

    private void mergeSort(int[] arr, int[] temp, int first, int last){
        if (first<last){
            int mid = (first+last)/2;
            mergeSort(arr, temp, first, mid);
            mergeSort(arr, temp, mid+1, last);
            sort(arr, temp, first, mid, last);
        }
    }

    private void sort(int[] arr, int[] temp, int first, int mid, int last){
        int left = first;
        int right = mid+1;
        int index = 0;
        while (left<=mid && right<=last){
            if (arr[left]>=arr[right]){
                temp[index++] = arr[right++];
            }else {
                temp[index++] = arr[left++];
            }
        }

        while (left<=mid){
            temp[index++] = arr[left++];
        }
        while (right<=last){
            temp[index++] = arr[right++];
        }
        index = 0;
        while(first <= last){
            arr[first++] = temp[index++];
        }
    }



}
