# Spark 核心篇 - SparkContext初始化流程

## 序

今天起详细分析和记录 Spark 相关组件的实现原理和代码分析。Spark 代码也写过一些了，对大概的使用流程有了简单的了解，不过对于
底层的实现，和一些原理性知识还不够明白。所以决定从源码入手认认真真的分析一下。自己大概制定了一个计划，我们从 Spark 最核心的 
Spark core 开始，以最重要的 SparkContext 为切入点，以整个 Spark 程序的流程为参考进行分析。大致流程如下：
1. SparkContext
2. RDD
3. DAGScheduler
	- Stage 划分
	- 本地缓存
4. TaskScheduler
	- 调度算法
	- 内部通信
5. checkpoint
6. Shuffle 和 BlockManager
7. 运行模式分析
8. 优化方案

## 进入正题

选择从 SparkContext 入手，是因为自己第一次写 Spark 代码就是`val context = new SparkContext(conf)`,当时没有多想，
只是觉得这应是开发 Spark 的入口类吧，很重要，一定要记住。在写了几次后就觉得这个东西到底是个什么，为什么每次一定要先由他开始
。兴趣使然我踉踉跄跄的打开 Spark 源码，不得不说 Spark 源码对于我一个学了没多久 Scala 的人看起来还是比较友好的，一是他整体
代码风格很规范，二是他的注释较多，而且很多解释了原因，以及原理。也正如此萌生了仔细分析一下源码的想法。

## 正式开始

(SC)SparkContext 作为Spark 的主要功能入口，sparkContext表示与spark集群的连接，可用于在该集群上创建RDD、累加器和广播变量。
也就是说，在我们编写 Spark 时，一切基于 RDD 的操作最后都是交由 SparkContext 进行的，这一点在介绍 SparkContext 创建 RDD
时详细介绍。

SC 作为主要的 spark 基础类，除了创建 RDD 外，他同时也是我们在提交了任务后，对于环境的初始化，以及 Spark 在运行过程中使用到的多个调度器的初始
化工作（DAGScheduler、TaskScheduler），用一张图来简单说明 SparkContext 的主要初始化工作。

![20190728150713344.png](https://files.sev7e0.site/images/oneblog/20190728150713344.png)

图片中主要描述了在 SparkContext 实例化的过程中主要做了那些环境准备工作,这些工作都是在 SparkContext 伴生类中进行的初始化,接下来详细介绍.

1. 初期 SparkContext 会维护一份`SparkConf`,这其中包括了`spark.master`,`spark.app.name`等一些基础信息的配置.
	
2. 在将`SparkConf`维护完后,第一件事是将Spark 内部使用的消息监听器进行初识化`LiveListenerBus`.
3. 之后使用`SparkConf`,监听器,进行`SparkEnv`初始化.
4. 如果运行repl，向文件服务器注册repl的输出目录。
5. 创建一个 Spark的状态跟踪对象,用于报告 Spark 的 job 和 stage 的状态的 API,不过该API 提供的的信息并不能实时和一致性,会存在状态存在而获取到空的状态,而且为了内存考虑,只保留了最近一段时间的相关状态.
6. 接下来的任务是初始化任务进度条,以及 `Spark UI`,用于访问任务执行页面.
7. 接下来是关于 Executor 的关键配置获取,并将获取到的配置缓存到内存中,以便后边在用到时直接调用,然后将 executor 的心跳接收器进行初始化,以便接受每个注册到这个任务的 executor 进行心跳检测.
8. =======================下边时 SparkContext 中初始化的关键步骤===================================
9. 创建`TaskScheduler`,注意的是在创建的同时还会创建`SchedulerBackend`,根据不同的运行模式,会自动创建个不同的调度器和 backend,具体的使用环境会在后边详细介绍.
10. 创建`DAGScheduler`,这个类的主要用途是根据 RDD 的 lineage 进行 stage 划分,以及根据不同的 stage 进行 job 的创建并提交给 TashScheduler.
11. 初始化`BlockManager`,使用 id 初始化 BlockManager 这其中做了关于 BlockManager 的相关操作.
12. 将主要环境和功能都初始化完成后,进行一些收尾工作,包含应用启动消息发送,环境更新事件发送等一些操作,此时任务执行前期的准备工作都已完成.

> 相关源码和注释请参考我的 [spark/SparkContext.scala at branch-2.4 · sev7e0/spark · GitHub](https://github.com/sev7e0/spark/blob/branch-2.4/core/src/main/scala/org/apache/spark/SparkContext.scala)
