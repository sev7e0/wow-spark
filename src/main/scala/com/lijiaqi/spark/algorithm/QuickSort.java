package com.lijiaqi.spark.algorithm;


/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-11 14:56
 **/

public class QuickSort {

    public static void main(String[] args) {
        int[] a = {1,5,4,2,3,6,7};
        quickSort(a,0,a.length-1);
    }

    public static void quickSort(int[] arr,int left, int right){

        if (left>=right){
            System.out.println("complete");
        }
        int index = partSort2(arr,left,right);
        quickSort(arr, left,index-1);
        quickSort(arr, index+1, right);
    }
    //左右指针法
    private static int partSort(int[] arr, int left, int right) {
        int index = right;
        int value = arr[index];

        while (left<right){
            while (left<right && arr[left]<=value){
                ++left;
            }
            while (left<right && arr[right]>=value){
                --right;
            }
            swap(arr, left, right);
        }
        swap(arr, left, index);
        return index;
    }
    //填坑方式
    private static int partSort2(int[] arr, int left, int right){
        int value = arr[right];

        while (left<right){
            while (left<right && arr[left]<=value){
                ++left;
            }
            arr[right] = arr[left];
            while (left<right && arr[right]>=value){
                --right;
            }
            arr[left] = arr[right];
        }
        arr[right] = value;

        return right;
    }


    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
//https://blog.csdn.net/qq_36528114/article/details/78667034

}
