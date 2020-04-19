package com.sev7e0.wow.core.scala

import com.sev7e0.wow.utils.TestUtils
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
 * Spark中相关的转换操作,delay operation,只有在Action(参看ActionOperation_Scala)
 * 操作之后才会真实的执行.
 *
 * Spark 根据操作属性分别 RDD 之间的依赖属性,再根据依赖属性进行 stage 的划分,最终将 stage
 * 封装成 job 进行任务的提交.
 *
 * 依赖属性:
 * 宽依赖(ShuffleDependency): 是指当前 rdd 的父RDD 被多个 RDD 引用
 * 窄依赖(NarrowDependency): 是指当前 rdd 的父RDD 只被一个 RDD 所引用
 *
 * 详细参看我的博客:
 */
class TransformationOperation_Scala extends TestUtils {

	/**
	 * 通过向这个RDD的所有元素应用一个函数来返回一个新的RDD。
	 */
	test("map test") {
		val listRdd = Array(1, 2, 3, 5, 8, 6)
		context.parallelize(listRdd).map(_ * 2).foreach(println(_))
	}


	/**
	 * 类似于map，但是是在每一个partition上运行.
	 * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,
	 * 而mapPartitions被调用M次,一个函数一次处理所有分区。
	 *
	 * 返回一个MapPartitionRDD
	 *
	 * @param context
	 */
		
		
	def mapPartitions(context: SparkContext): Unit = {
		val value = context.parallelize(1 to 10, 3)

		//构造function，参数和返回值只能是Iterator类型
		def mapPartitionsFunction(iterator: Iterator[Int]): Iterator[(Int, Int)] = {
			var result = List[(Int, Int)]()
			while (iterator.hasNext) {
				val i = iterator.next()
				//在list开头插入新的tuple，由于是不可变得，所以生成了一个新的list
				result = result.::(i, i * i)
			}
			result.iterator
		}

		val listValue = context.parallelize(List((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)))
		//向mapPartitions传递function
		val partitionRDD = value.mapPartitions(mapPartitionsFunction)
		partitionRDD.foreach(out => println(out))
		//(3,9)
		//(2,4)
		//(1,1)
		//(6,36)
		//(5,25)
		//(4,16)
		//(10,100)
		//(9,81)
		//(8,64)
		//(7,49)

		//函数式写法 生成新的(a , b*b)
		val tupleRDD = listValue.mapPartitions(iterator => {
			var resultTuple = List[(Int, Int)]()
			while (iterator.hasNext) {
				val tuple = iterator.next()
				resultTuple = resultTuple.::(tuple._1, tuple._2 * tuple._2)
			}
			resultTuple.iterator
		})
		tupleRDD.foreach(println(_))
		//(9,81)
		//(8,64)
		//(7,49)
		//(2,9)
		//(2,4)
		//(2,1)
		//(1,9)
		//(1,4)
		//(1,1)
	}

	/**
	 * reduceByKey根据关联，指定函数合并每一个key的值
	 *
	 * reduceByKey将在mapper端进行本地合并，之后将合并后的数据发送到reducer，类似于MapReduce中的combiner
	 * 输出将使用现有的分区器/并行级别进行哈希分区。
	 *
	 */
	test("reduceByKey test") {
		val value = context.parallelize(List(1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4))
		value.map(num => (num, 1)).reduceByKey(_ + _ * 2).foreach(println(_))
		//输出
		//(4,3)
		//(1,7)
		//(3,5)
		//(2,5)
	}

	/**
	 * 返回一个新的RDD，方法是首先将一个函数应用于这个RDD的所有元素，然后将结果展平。
	 */

	 test("flatMap"){
		val listRdd = Array("hello you ", "hello java", "hello leo")
		context.parallelize(listRdd).flatMap(_.split(" ")).foreach(println(_))
	}

	/**
	 * 过滤掉不符合要求的元素，返回一个新的 rdd
	 */
	test("filter test"){
		val listRdd = Array(1, 2, 3, 5, 8, 6, 7, 8, 9, 10)
		context.parallelize(listRdd).filter(_ % 2 != 0).foreach(println(_))
	}
	
	/**
	 * 将value的值根据key进行分组，并返回一个序列。不能保证生成元素的顺序。
	 *
	 * 该操作十份消耗性能，如果为了对每个键执行*聚合（如总和或平均值）而进行分组，
	 * 则使用“PairRDDFunctions.aggregateByKey”或“PairRDDFunctions.reduceByKey”将提供更好的性能。
	 */
	test("groupByKey test") {
		val listRdd = Array(new Tuple2[String, Integer]("class1", 50),
			new Tuple2[String, Integer]("class1", 80),
			new Tuple2[String, Integer]("class2", 65),
			new Tuple2[String, Integer]("class3", 580),
			new Tuple2[String, Integer]("class1", 75))
		context.parallelize(listRdd).groupByKey().foreach(rdd => {
			println(rdd._1)
			for (score <- rdd._2) println(score)
		})
	}
		
		test("aggregateByKey") {
			val pairs = context.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 2)
			
			val sets = pairs.aggregateByKey(new mutable.HashSet[Int]())(_ += _, _ ++= _).collect()
			assert(sets.length === 3)
			val valuesFor1 = sets.find(_._1 == 1).get._2
			assert(valuesFor1.toList.sorted === List(1))
			val valuesFor3 = sets.find(_._1 == 3).get._2
			assert(valuesFor3.toList.sorted === List(2))
			val valuesFor5 = sets.find(_._1 == 5).get._2
			assert(valuesFor5.toList.sorted === List(1, 3))
		}
	
	/**
	 * combineByKey函数
	 * def combineByKey[C](
	 * 			createCombiner: V => C,
	 * 			mergeValue: (C, V) => C,
	 *	 		mergeCombiners: (C, C) => C)
	 *
	 *  1.createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。
	 *  如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值
	 *
	 * 	2.mergeValue: 如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
	 *
	 * 	3.mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器，
	 * 	就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
	 *
	 * In addition, users can control the partitioning of the output RDD, and whether to perform
	 * map-side aggregation (if a mapper can produce multiple items with the same key).
	 */
	test("combineByKey test"){
		val pairs = context.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 2)
		val valueRDD = pairs.combineByKey(
			v => (1, v),
			(v: (Int, Int), nv) => (v._1 + 1, v._2 + nv),
			(v: (Int, Int), nv: (Int, Int)) => (v._1 + nv._1, v._2 + nv._2))
		valueRDD.foreach(println(_))
		//(1,(2,2))
		//(3,(1,2))
		//(5,(2,4))
		val pair = context.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 3)
		val value_rdd = pair.combineByKey(
			v => List(v),
			(v: List[Int], nv:Int) => v.::(nv),
			(v: List[Int], nv: List[Int]) => v.:::(nv))
		value_rdd.foreach(println(_))
		//(1,List(1, 1))
		//(3,List(2))
		//(5,List(3, 1))
	}
	
	/**
	 * 对两个rdd根据key进行关联，inner join也就意味着没有匹配上的key将会被过滤调
	 */
	test("join test"){
		val scoreList = Array(new Tuple2[Integer, Integer](1, 25),
			new Tuple2[Integer, Integer](2, 89),
			new Tuple2[Integer, Integer](3, 100),
			new Tuple2[Integer, Integer](4, 75),
			//key 6将会被过滤调，因为在nameList中不存在key 6
			new Tuple2[Integer, Integer](6, 75),
			new Tuple2[Integer, Integer](5, 0))
		val nameList = Array(new Tuple2[Integer, String](1, "leo"),
			new Tuple2[Integer, String](2, "json"),
			new Tuple2[Integer, String](3, "spark"),
			new Tuple2[Integer, String](4, "fire"),
			new Tuple2[Integer, String](5, "samsung"))
		context.parallelize(nameList)
			.join(context.parallelize(scoreList))
			.foreach(scoreName => {
				println(scoreName._1+":"+scoreName._2._1+"-"+scoreName._2._2)
			})
	}
	/**
	 * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
	 * list of values for that key in `this` as well as `other`.
	 *
	 */
	test("cogroups test") {
		val scoreList = Array(new Tuple2[Integer, Integer](1, 25),
			new Tuple2[Integer, Integer](2, 89),
			new Tuple2[Integer, Integer](3, 100),
			new Tuple2[Integer, Integer](3, 75),
			new Tuple2[Integer, Integer](1, 0))
		val nameList = Array(new Tuple2[Integer, String](1, "leo"),
			new Tuple2[Integer, String](2, "json"),
			new Tuple2[Integer, String](3, "spark"),
			new Tuple2[Integer, String](2, "fire"),
			new Tuple2[Integer, String](1, "samsung"))

		context.parallelize(nameList)
			.cogroup(context.parallelize(scoreList))
			//scala中输入多行需要使用 { } 进行包裹，否则只能使用一行处理
			.foreach(score => {
				println(score._1);
				println(score._2._1);
				println(score._2._2);
				println("----------")
			})

	}
}














