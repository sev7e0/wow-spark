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

### SparkConf
初期 SparkContext 会维护一份`SparkConf`, 是在构造 SparkContext 时传递进来的,这其中包括了`spark.master`,`spark.app.name`等一些基础信息的配置.
对他们进行一些校验,而且这份`SparkConf`在运行时是不可变得,在配置和校验时使用的都是他的拷贝副本.

### LiveListenerBus
在将`SparkConf`维护完后,第一件事是将Spark 内部使用的事件总线进行初识化`LiveListenerBus`,`LiveListenerBus`继承`ListenerBus`,他是 Spark 中众多消息
总线中一员,他会在 SparkContext 存在期间一直存在,并且使用异步的方式对产生的事件发送给监听器.Spark 使用监听器模式,完成了 Spark 集群的大部分事件传递.是非常
重要的设计模式.

### AppStatusStore
AppStatusStore提供Spark程序运行中各项监控指标的键值对化存储。Web UI中见到的数据指标基本都存储在这里。其初始化代码如下。
```scala
_statusStore = AppStatusStore.createLiveStore(conf)
listenerBus.addToStatusQueue(_statusStore.listener.get)
```
```scala
/**
   * Create an in-memory store for a live application.
   */
  def createLiveStore(conf: SparkConf): AppStatusStore = {
    val store = new ElementTrackingStore(new InMemoryStore(), conf)
    val listener = new AppStatusListener(store, conf, true)
    new AppStatusStore(store, listener = Some(listener))
  }
```
AppStatusStore底层使用了ElementTrackingStore，它是能够跟踪元素及其数量的键值对存储结构，因此适合用于监控。另外还会产生一个监听器AppStatusListener的实例，
并注册到前述LiveListenerBus中，用来收集监控数据。

### SparkEnv

SparkEnv 主要作用是,为正在运行的Spark实例（master或worker）保存所有运行时环境对象，包括序列化程序、rpcenv、块管理器、映射输出跟踪器等。当前Spark代码通过
全局变量查找Sparkenv，因此所有线程都可以访问相同的Sparkenv。sparkenv.get可以访问它（例如，在创建sparkcontext之后）
```scala
private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master, conf))
  }
```
从源码中可以见到,他同样依赖于 SparkConf 和 LiveListenerBus,而且调用的时`createDriverEnv`,进入 SparkEnv 源码实现,可以见到他还可以创建`ExecutorEnv`这在
后边文章中在详细介绍.

### SparkStatusTracker
创建一个 Spark的状态跟踪对象,用于报告 Spark 的 job 和 stage 的状态的 API,不过该API 提供的的信息并不能实时和一致性,会存在状态存在而获取到空的状态,而且为了内存
考虑,只保留了最近一段时间的相关状态.


### ConsoleProgressBar && SparkUI
```scala
//初始化任务进度条
_progressBar =
	if (_conf.get(UI_SHOW_CONSOLE_PROGRESS) && !log.isInfoEnabled) {
		Some(new ConsoleProgressBar(this))
	} else {
		None
	}
//初始化 Spark UI
_ui =
	if (conf.getBoolean("spark.ui.enabled", true)) {
		Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "",
			startTime))
	} else {
		// For tests, do not enable the UI
		None
	}
// Bind the UI before starting the task scheduler to communicate
// the bound port to the cluster manager properly
_ui.foreach(_.bind())
```
从名字就可以看出来他们的主要工作就是创建任务进度条和 Spark 任务执行界面.


### executorEnvs
接下来是关于 Executor 的关键配置获取,并将获取到的配置缓存到内存中,以便后边在用到时直接调用,然后将 executor 的心跳接收器进行初始化,以便接受每个注册到这个任务的 executor 进行心跳检测.

### HeartbeatReceiver
HeartbeatReceiver是心跳接收器。Executor需要向Driver定期发送心跳包来表示自己存活。它本质上也是个监听器，继承了SparkListener。其初始化代码如下。
代码#2.9 - 构造方法中HeartbeatReceiver的初始化
```scala
_heartbeatReceiver = env.rpcEnv.setupEndpoint(
HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
```
可见，HeartbeatReceiver通过RpcEnv最终包装成了一个RPC端点的引用.
Spark集群的节点间必然会涉及大量的网络通信，心跳机制只是其中的一方面而已。因此RPC框架同事件总线一样，是Spark底层不可或缺的组成部分。

 =======================下边时 SparkContext 中初始化的关键步骤===================================
 
 
### TaskScheduler && SchedulerBackend

TaskScheduler 是一个 trait,不过他只有 TaskSchedulerImpl 一种实现方式,在 Spark 任务调度中主要负责为 Task 进行调度,不同的需求提供不同的调度算法.并且通过
内部持有的 SchedulerBackend 进行工作.
SchedulerBackend负责向等待计算的Task分配计算资源，并在Executor上启动Task。它是一个Scala特征，有多种部署模式下的SchedulerBackend实现类,不同的运行模式要
使用不同的 Backend,我们上边图中只展示了SparkDeploySchedulerBackend。它在SparkContext中是和TaskScheduler一起初始化的，作为一个元组返回。

### DAGScheduler

DAGScheduler即有向无环图（DAG）调度器。DAG用来表示RDD之间的 lineage 。DAGScheduler负责生成并提交Job，以及按照DAG将RDD和算子划分并提交Stage。每个Stage
都包含一组Task，称为TaskSet，它们被传递给TaskScheduler。也就是说DAGScheduler需要先于TaskScheduler进行调度。

DAGScheduler初始化是直接new出来的，但在其构造方法里也会将SparkContext中TaskScheduler的引用传进去。因此要等DAGScheduler创建后，再真正启动TaskScheduler。

```scala
_dagScheduler = new DAGScheduler(this)
_heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)
_taskScheduler.start()
```
### BlockManager
初始化`BlockManager`,使用 id 初始化 BlockManager 这其中做了关于 BlockManager 的相关操作.

```scala
_env.blockManager.initialize(_applicationId)
```

### ContextCleaner
ContextCleaner即上下文清理器。它可以通过spark.cleaner.referenceTracking参数控制开关，默认值true。它内部维护着对RDD、Shuffle依赖和广播变量（之后会提到）
的弱引用，如果弱引用的对象超出程序的作用域，就异步地将它们清理掉。以此来坚守内存占用.

### final
将主要环境和功能都初始化完成后,进行一些收尾工作,包含应用启动消息发送,环境更新事件发送等一些操作,此时任务执行前期的准备工作都已完成.

> 相关源码和注释请参考我的 [spark/SparkContext.scala at branch-2.4 · sev7e0/spark · GitHub](https://github.com/sev7e0/spark/blob/branch-2.4/core/src/main/scala/org/apache/spark/SparkContext.scala)
