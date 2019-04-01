package com.sev7e0.spark.algorithm;

/**
 * @description: TODO
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-26 14:35
 **/

/**
 * 中心思想:它是一种简单直观的排序算法。它的工作原理如下。首先在未排序序列中找到最小（大）元素，存放到排序序列的起始位置，然后，再从剩余未排序元素中继续寻找最小（大）元素，然后放到已排序序列的末尾。
 *          以此类推，直到所有元素均排序完毕。
 *          它的主要优点与数据移动有关。如果某个元素位于正确的最终位置上，则它不会被移动。选择排序每次交换一对元素，它们当中至少有一个将被移到其最终位置上，因此对n个元素的表进行排序总共进行至多n-1次交换。
 *          在所有的完全依靠交换去移动元素的排序方法中，选择排序属于非常好的一种
 * 归并优化:对于小数组可以采用和快排一样的方式,使用插入排序或者选择排序.
 * 说明:归并排序的时间复杂度是O(NLogN)，空间复杂度是O(N)。
 *      辅助数组是一个共用的数组。如果在每个归并的过程中都申请一个临时数组会造成比较大的时间开销。
 *      归并的过程需要将元素复制到辅助数组，再从辅助数组排序复制回原数组，会拖慢排序速度。
 */

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
            int mid = (first+last)>>1;
            mergeSort(arr, temp, first, mid);
            mergeSort(arr, temp, mid+1, last);
            sort(arr, temp, first, mid, last);
        }
    }

    private void sort(int[] arr, int[] temp, int first, int mid, int last){
        int left = first;
        int right = mid+1;
        int index = 0;
        //从两个数组中依次比较,放入到temp中,知道其中一个数组遍历结束
        while (left<=mid && right<=last){
            if (arr[left]>=arr[right]){
                temp[index++] = arr[right++];
            }else {
                temp[index++] = arr[left++];
            }
        }
        //左边数组为遍历结束,则将数组中剩余的元素全部加入到temp中
        while (left<=mid){
            temp[index++] = arr[left++];
        }
        //右边同理
        while (right<=last){
            temp[index++] = arr[right++];
        }
        index = 0;
        while(first <= last){
            arr[first++] = temp[index++];
        }
    }



}
