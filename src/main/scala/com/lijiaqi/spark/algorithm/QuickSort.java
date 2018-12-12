package com.lijiaqi.spark.algorithm;


/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-11 14:56
 **/

/**
 * 主要思想:如果待排序的长度每趟排序选取一个基准值,以该值为中心将未排序的序列进行分割,比该值小的放在左侧,比该值大的放在右侧,
 *         并持续迭代,直到迭代的序列长度<2
 * 实现方式:迭代过程中每次进行一次快排可以有几种实现方式
 *         第一种:左右指针;第二种:填坑方式,第三种:前后指针.具体细节到代码实现领会
 * 快排优化:关于优化可以从以下几个方面进行考虑
 *         1.基准值选取:如算法书描述的,默认选取第一个或者最后一个作为基准值是很愚蠢的行为,在待排序列默认有序的情况下,将会产生最坏的情况.
 *         两种方式:0.在待排序列中选取随机数,这样在一定概率的情况下解决了之前存在的问题.
 *                 1.三数取中方式(推荐),我们选取第一个,最后一个,序列中间三个数中的中值,是选取的基准值均衡的概率更大.
 *         2.子序列的排序:快排作为递归排序在针对小序列(10左右)时,效率不如插入排序来得快,所以我们在实现快速排序时,在子序列达到了一定小时,
 *         我们可以考虑使用插入排序替代快排.
 *         3.基于基准值的区域划分:在选取了合适的基准值后，需要考虑的问题是，如果待排序列中存在了多个重复值，或者说全部都是重复值，那将会再次
 *         算法的最坏情况，这时我们可以新增一块存放于基准值相等的区域，这种方式又叫做'三路快排'/'三路划分',在将序列中其他值排好序后,将改序列插入即可.
 *
 */
public class QuickSort {

    public static void main(String[] args) {
        int[] a = {1,5,4,6,3,2,7};
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
        selectMid(arr);
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
    //填坑法
    private static int partSort2(int[] arr, int left, int right){
        selectMid(arr);
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
    //选取中间值的方法
    private static void selectMid(int[] arr){
        int lenth = arr.length-1;
        int head = arr[0];
        int tail = arr[lenth];
        int mid = arr[(lenth)>>1];

        int max = head > mid ? head : mid;
        max = max > tail ? max : tail;
        int min = head > mid ? mid : head;
        min = min > tail ? tail : min;
        //使用最后一个承接
        tail = head + tail + mid - max -min;

        if (tail == head){
            swap(arr, 0, lenth);
        }else if (tail == mid){
            swap(arr,(lenth)>>1,lenth);
        }
    }

    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
//https://blog.csdn.net/qq_36528114/article/details/78667034

}
