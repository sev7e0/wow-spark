package com.sev7e0.spark.algorithm;

/**
 * @description:
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-12-14 16:02
 **/

/**
 * 中心思想:采用双层遍历+倒序比较实现,默认最左边的为有序数列第一层遍历默认指针指向第二位元素,然后开始第二层遍历,第二层遍历主体是有序数列,
 *          大于当前值,指针前移,直到找到第一个小于当前值的位置,遍历停止.第一层遍历指针前移继续比较.
 * 插排优化:在寻找当前值位置时,可以使用二分查找.
 */
public class InsertSort {

    public static void main(String[] args) {
        TimerUtil.printChart();
    }

    @Timer
    private static void insertSort(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        for (int i = 1; i < arr.length; i++) {
            int temp = arr[i];
            int j;
            for (j=i-1; j>=0 && arr[j]>temp; j--){
                //有序数组中的当前值大于temp时,当前值后移,继续比较
                arr[j+1]=arr[j];
            }
            //
            arr[j+1]=temp;
        }
        AlgorithmUtil.print(arr);
    }
    //优化后:二分插入排序
    @Timer
    private static void insertSortByBinarySearch(){
        int[] arr = AlgorithmUtil.getRepeatArray();
        for (int i = 1; i < arr.length; i++) {
            //找到有序数列的起始位置
            int low = 0;
            int heigh = i-1;
            int temp = arr[i];
            //这里注意要添加起始点等于判断
            while (heigh >= low){
                //根据起始位置找到中间位置
                int mid = (low+heigh)>>1;
                if (arr[mid]>temp){
                    heigh = mid-1;
                }else {
                    //当相等或者小于temp时,合适的插入位将在mid之后的位置
                    low = mid+1;
                }
            }
            //将low开始到i的每一个元素后移,完成后low和low+1是相等的
            System.arraycopy(arr, low, arr, low + 1, i - low);
//            for (int j=i; j>low;j--){
//                arr[j] = arr[j-1];
//            }
            //将temp插入到low的位置,此次插入结束
            arr[low] = temp;
        }
        AlgorithmUtil.print(arr);
    }

}
