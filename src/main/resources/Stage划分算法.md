## Stage划分算法
1. 首先通过getOrCreateShuffleMapStage创建一个FinalStage

```scala
private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        //根据给定的shuffleDep创建一个stage,并返回
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }
```
2. 通过submitStage将FinalStage进行提交,由此开始了Stage划分算法的重要步骤
```scala
/** Submits stage, but first recursively submits any missing parents. */
  //想递归出缺少父依赖
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      //先去确认当前stage不在stage等待队列,stage执行队列以及失败队列
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        //获取当前stage缺少的父依赖父stage
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        //缺少的依赖集合为空,则开始准备提交
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          //在这里边调用了submitWaitingChildStages该方法,将所有等调度的stage进行提交
          submitMissingTasks(stage, jobId.get)
        } else {
          //如果父stage不为空的话,递归调用在去寻找该stage的父stage,知道不存在缺少的父依赖
          for (parent <- missing) {
            submitStage(parent)
          }
          //并且将该stage放入等待调用的队列
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }
```
3. 由getMissingParentStages可以知道,从刚刚传递进来的Finalstage中获取RDD,遍历每一个rdd,若为宽依赖则创建一个ShuffleMapStage,
若为一个窄依赖,则推进栈中,进行下一次遍历,知道遇到一个宽依赖将stage返回

```scala
private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    //使用Stack结构
    val waitingForVisit = new ArrayStack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              //如果是宽依赖,那么久创建一个shuffleMapStage
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                //如果没有找到这个stage
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
                //如果是窄依赖推入栈中
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    //若当前等待遍历的堆栈不为空,则继续出栈开始寻找
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }
```
4. 返回后的stage会判断是否为空,若为空则表明该stage没有父stage,若不为空将会遍历这个stage集合,然后递归的取寻找每一个父stage,知道全部为空时,提交stage,进行下一步的task创建
```scala

```