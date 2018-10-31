#!/bin/bash
#任务提交脚本,第一个参数传递入口类路径,第二个参数传递jar文件名
#例如:./run.sh com.lijiaqi.spark.core.LineCount_Scala spark-learn-1.0-SNAPSHOT.jar

spark-submit \
--class $1 \
--num-executors 3 \
--driver-memory 1000m \
--executor-memory 100m \
--executor-cores 3 \
$2