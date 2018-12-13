package com.lijiaqi.spark.algorithm;


/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-11 14:56
 **/

import java.util.Arrays;
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
 * 其他实现:双基准排
 */
public class QuickSort {

    public static void main(String[] args) {
        int[] a = {1,5,4,7,3,25,5,5,6,6,6,6,5,5,6,6};

        quickSort(a,0,a.length-1);
    }

    private static void quickSort(int[] arr,int left, int right){
        if (left>=right){
            return;
        }
        int index = partSort(arr,left,right);
        quickSort(arr, left,index-1);
        quickSort(arr, index+1, right);
        print(arr);
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
    //填坑法
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

    /**
     * 三路快排的实现
     * @param nums 待排序列
     * @param left 待排序列最左索引值
     * @param right 待排序列最右索引值
     */
    private static void sort3(int[] nums, int left, int right){
        //当左右指针相遇时直接返回
        if (left >= right){
            return;
        }
        //使用指针i作为与基准值相等的元素的区域边界
        int lt=left,gt=right,i =right-1;
        //这里才有最右元素作为基准值,也就是说i <- gt这片区域都将存储与基准值相等的元素
        int value = nums[right];
        //以下是初始状态 { 1 , 5 , 4 , 7 , 3 , 25 ,    5    }
        //              lt                   i   gt/value
        //过程主要是nums[i]与value比较
        while (i >= lt){
            // 大于value的值扔到gt后边,gt前移
            if (nums[i] > value){
                swap(nums, i--, gt--);
            }
            // 小于value的值扔到lt前边,lt后移
            else if(nums[i] < value){
                swap(nums, i, lt++);
            }
            //等于情况下不操作,直接i前移
            else {
                i--;
            }
        }
        //开始分区域迭代,同时基准值相等的元素的区域不参与迭代
        sort3(nums, left, lt - 1);
        sort3(nums, gt, right);
        print(nums);
    }


    private void sort(int[] input, int lowIndex, int highIndex) {

        if (highIndex<=lowIndex) return;

        int pivot1=input[lowIndex];
        int pivot2=input[highIndex];

        if (pivot1>pivot2){
            swap(input, lowIndex, highIndex);
            pivot1=input[lowIndex];
            pivot2=input[highIndex];
        }
        else if (pivot1==pivot2){
            while (pivot1==pivot2 && lowIndex<highIndex){
                lowIndex++;
                pivot1=input[lowIndex];
            }
        }

        int i=lowIndex+1;
        int lt=lowIndex+1;
        int gt=highIndex-1;

        while (i<=gt){

            if (input[i]> pivot1){
                swap(input, i++, lt++);
            }
            else if (pivot2> input[i]){
                swap(input, i, gt--);
            }
            else{
                i++;
            }

        }

        swap(input, lowIndex, --lt);
        swap(input, highIndex, ++gt);

        sort(input, lowIndex, lt-1);
        sort (input, lt+1, gt-1);
        sort(input, gt+1, highIndex);

    }
    //选取中间值的方法
    private static int[] selectMid(int[] arr){
        if (arr.length<3){
            return arr;
        }
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
        return arr;
    }

    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
    //https://blog.csdn.net/qq_36528114/article/details/78667034
    private static void print(int[] arr){
        //注意Arrays.asList()返回的是一个被包装的List(实质是数组),所有对应的操作最后都会直接操作数组
        //使用时，当传入基本数据类型的数组时，会出现小问题，会把传入的数组整个当作返回的List中的第一个元素
        //这里使用Arrays.stream()
        Arrays.stream(arr).forEach(System.out::print);
        System.out.println();
    }

}
