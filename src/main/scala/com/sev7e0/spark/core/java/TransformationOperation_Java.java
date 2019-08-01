package com.sev7e0.spark.core.java;


import com.sev7e0.spark.JavaSparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Transformation算子
 */
@SuppressWarnings({"unchecked", "unused"})
public class TransformationOperation_Java {

    public static void main(String[] args) {
        //通过工具类获得context
        JavaSparkContext context = new JavaSparkContextUtil("TransformationOperation_Java", "local").getContext();
        coGroup(context);
        join(context);
        gropByKey(context);
        flatMap(context);
        filter(context);
        map(context);
    }

    /**
     * cogroup会根据rdd的key将所有的valve值进行聚合
     * 只不过每个rdd会是一个独立的Iterable
     * @param context
     */
    private static void coGroup(JavaSparkContext context) {
        List<Tuple2<Integer, String>> tuple2List = Arrays.asList(new Tuple2<>(1, "leo"),
                new Tuple2<>(2, "json"),
                new Tuple2<>(3, "spark"),
                new Tuple2<>(2, "fire"),
                new Tuple2<>(1, "samsung"));
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(new Tuple2<>(1, 25),
                new Tuple2<>(2, 89),
                new Tuple2<>(3, 100),
                new Tuple2<>(3, 75),
                new Tuple2<>(1, 0));

        JavaPairRDD<Integer, String> nameRdd = context.parallelizePairs(tuple2List);
        JavaPairRDD<Integer, Integer> scoreRdd = context.parallelizePairs(scores);
        //join以后会根据两个rdd的key进行返回一个新的PairRdd
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2JavaPairRDD = nameRdd.cogroup(scoreRdd);
        integerTuple2JavaPairRDD.foreach((VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>) integerTuple2Tuple2 -> {
            System.out.println(integerTuple2Tuple2._1);
            System.out.println(integerTuple2Tuple2._2._1);
            System.out.println(integerTuple2Tuple2._2._2);
            System.out.println("-------------------------");
            /**
             * 输出
             * 1
             * [leo, samsung]
             * [25, 0]
             * -------------------------
             * 3
             * [spark]
             * [100, 75]
             * -------------------------
             * 2
             * [json, fire]
             * [89]
             * -------------------------
             */
        });
    }

    /**
     * join算子可以将两个rdd进行关联
     * @param context
     */
    private static void join(JavaSparkContext context) {
        List<Tuple2<Integer, String>> tuple2List = Arrays.asList(new Tuple2<>(1, "leo"),
                new Tuple2<>(2, "json"),
                new Tuple2<>(3, "spark"),
                new Tuple2<>(4, "fire"),
                new Tuple2<>(5, "samsung"));
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(new Tuple2<>(1, 25),
                new Tuple2<>(2, 89),
                new Tuple2<>(3, 100),
                new Tuple2<>(4, 75),
                new Tuple2<>(5, 0));

        JavaPairRDD<Integer, String> nameRdd = context.parallelizePairs(tuple2List);
        JavaPairRDD<Integer, Integer> scoreRdd = context.parallelizePairs(scores);
        //join以后会根据两个rdd的key进行返回一个新的PairRdd
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = nameRdd.join(scoreRdd);
        join.foreach((VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>) integerTuple2Tuple2 -> {
            System.out.println(integerTuple2Tuple2._1);
            System.out.println(integerTuple2Tuple2._2._1);
            System.out.println(integerTuple2Tuple2._2._2);
            System.out.println("-------------------------");
            /**
             * 输出：
             * 4
             * fire
             * 75
             * -------------------------
             * 1
             * leo
             * 25
             * -------------------------
             * 3
             * spark
             * 100
             * -------------------------
             * 5
             * samsung
             * 0
             * -------------------------
             * 2
             * json
             * 89
             * -------------------------
             */
        });
    }

    /**
     * gropByKey算子实例:按照班级对成绩进行分组
     */
    private static void gropByKey(JavaSparkContext context) {

        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("class1", 50),
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 65),
                new Tuple2<>("class3", 580),
                new Tuple2<>("class1", 75));

        JavaPairRDD<String, Integer> javaPairRDD = context.parallelizePairs(list);
        //返回的还是一个JavaPairRDD,但是将value值进行了聚合,返回了一个Iterable
        JavaPairRDD<String, Iterable<Integer>> iterableJavaPairRDD = javaPairRDD.groupByKey();
        iterableJavaPairRDD.foreach((VoidFunction<Tuple2<String, Iterable<Integer>>>) stringIterableTuple2 -> {
            System.out.println(stringIterableTuple2._1);
            stringIterableTuple2._2.forEach(System.out::println);
            System.out.println();
            /**
             * class3
             * 580
             *
             * class1
             * 50
             * 80
             * 75
             *
             * class2
             * 65
             *
             */
        });
    }


    //将文本行进行拆分的算子
    private static void flatMap(JavaSparkContext context) {
        List<String> stringList = Arrays.asList("hello you ", "hello java", "hello leo");
        //返回的是一个Iterator,可以考虑使用List转换数组
        JavaRDD<String> flatMap = context.parallelize(stringList).flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        flatMap.foreach((VoidFunction<String>) s -> System.out.println(s + " "));
        /**
         * hello
         * you
         * hello
         * java
         * hello
         * leo
         */
    }

    /**
     * 使用filter算子,过滤不满足的元素
     * @param context
     */
    private static void filter(JavaSparkContext context) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //filter算子在返回时,若判断结果为true 保留当前结果,若为false则进行抛弃,返回新的rdd
        JavaRDD<Integer> filter = context.parallelize(list).filter((Function<Integer, Boolean>) integer -> integer % 2 != 0);

        filter.foreach((VoidFunction<Integer>) integer -> System.out.printf(integer + " "));
        System.out.println();
        /**
         * output:
         * 1 3 5 7 9
         */

    }

    /**
     * 根据指定函数，生成新的 rdd
     * @param context
     */
    private static void map(JavaSparkContext context) {

        //构造集合
        List<Integer> list = Arrays.asList(2, 3, 4, 5, 6, 5, 6, 5, 6, 5);

        //并行化集合,创建初始RDD

        JavaRDD<Integer> javaRDD = context.parallelize(list);

        /**
         * map算子,任何rdd类型都可以调用
         * 在java中map算子,接受的对象是Function
         * Function需要传递两个范型参数
         * 第一个call方法所需参数类型,第二个为call方法返回类型
         * 可以在call中对RDD中每一铬元素进行运算
         *
         */
        JavaRDD<Integer> integerJavaRDD = javaRDD.map((Function<Integer, Integer>) integer -> integer * 2);
        //遍历打印
        integerJavaRDD.foreach((VoidFunction<Integer>) integer -> System.out.print(integer + " "));
        //关闭context
        context.close();
        /**
         * output:
         *  4,6,8,10,12,10,12,10,12,10,
         */

    }

}