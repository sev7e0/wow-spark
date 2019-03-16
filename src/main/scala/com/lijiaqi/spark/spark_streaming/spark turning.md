### Spark 性能调优
#### process time优化
1. 调整数据接收的并行度：例如在kafka中一个kafka inputDStream同时接受两个kafka topic，这样我们就可以将其调整为两个inputDStream，接收后使用```streamingContext.union(kafkaStreams)```进行结合，统一成一个DStream.应考虑的另一个参数是接收器的块间隔，对于大多数接收器都是将接收到的数据进行聚合成block然后再存进内存，每个批次的block数量决定了转换操作的task的数量，每批接收程序的任务数大约为（批间隔/块间隔）。如果任务数量太少（少于每台计算机的核心数量），那么将效率低下，因为所有可用的核心都不会用于处理数据。要增加给定批处理间隔的任务数，减少块间隔。但是，建议的块间隔最小值约为50 ms，低于该值，任务启动开销可能是一个问题。
另一种解决方案是：针对集群资源使用repartition，将接受的数据重新分区，再开始处理。
2. 提高数据处理的并行级别：任何阶段如果task并行数量不够，那么将会导致资源利用率不足，可以通过```spark.default.parallelism```进行修改默认值。
3. 数据序列化：可以通过调整序列化方式，进而减小开销，推荐使用Kryo进行序列化，能够有效减少CPU和memory开销。在流应用程序需要保留的数据量不大的特定情况下，可以将数据（两种类型）作为反序列化对象进行持久化，而不会产生过多的GC开销。 spark中两种数据将会被序列化，1.输入数据2.流操作生成的持久化RDD http://spark.apache.org/docs/latest/streaming-programming-guide.html#data-serialization
4. task加载开销：如果每秒启动的任务数量过大将会产生很大的开销，而在独立模式或粗粒度细观模式下运行spark会比细粒度细观模式产生更好的任务启动时间。

#### batch size调整
1. 要使群集上运行的Spark Streaming应用程序保持稳定，系统应该能够以接收数据的速度处理数据。换句话说，批处理数据应该在生成时尽快处理。通过监视流式Web UI中的处理时间可以找到是否适用于应用程序，其中批处理时间应小于批处理间隔。 
2. 根据流式计算的性质，所使用的批处理间隔可能对应用程序在固定的一组集群资源上可以维持的数据速率产生重大影响。要验证系统是否能够跟上数据速率，您可以检查每个已处理批处理所遇到的端到端延迟的值（在Spark驱动程序log4j日志中查找“总延迟”，或使用```StreamingListener```接口）。
3. 如果延迟保持与批量大小相当，则系统稳定。否则，如果延迟不断增加，则意味着系统无法跟上，因此不稳定。一旦了解了稳定的配置，就可以尝试提高数据速率和/或减小批量。注意，只要延迟减小到低值（即，小于批量），由于临时数据速率增加引起的延迟的瞬时增加可能是正常的

#### memory调优
1. Spark应用对集群内存资源的使用一大部分取决于transformations的操作类型，例如一些跨大窗口操作，或者类似于```updateStateByKey```等操作将会大量使用内存资源，相反一些map-filter操作就不会。
2. 通常情况下由接收器接收的数据将会使用```StorageLevel.MEMORY_AND_DISK_SER_2```存储策略，这样数据在内存不足时自动的会保存到磁盘中，但也如此会降低spark的性能（io原因），所以官方建议的时尽量在spark的集群提供大的内存，这里可以考虑使用Alluxio 这种基于内存的分布式存储系统，能够大大提升Spark的效率
3. 使用java和scala那么就离不开一个问题JVM的辣鸡:chicken:回收，作为流式数据处理应用低延迟是必须的，肯定不希望stop the world这种事情发生，或者发生的时间尽可能的短，介绍一些参数用于调优内存和GC。
   * DStream持久化等级：spark中inputdata和RDD的持久化默认是序列化的，相比于序列化这样能够减少内存使用和GC开销。前边提过使用Kryo能够减少更多的开销，不过为了减少更多的内存再用，可以开启内存数据的压缩功能，```spark.rdd.compress```缺点是会使用更多的CPU时间
   * 清理旧数据：默认情况下，input data和持久化的RDD会自动的清除，由spark决定了何时清理这些数据，当然也可是长时间的保留数据，可以使用```streamingContext.remember```
   * 使用CMS垃圾收集器：强烈建议使用concurrent mark-and-sweep GC，以保持GC的暂停时间。尽管已知并发GC会降低系统的整体处理吞吐量，但仍建议使用它来实现更一致的批处理时间CMS GC。

#### others
1. DStream与单个接收器相关联。为了并行接收数据，需要创建多个接收器，即多个DStream。接收器在执行器内运行。它占据一个核心。要确保核心数能够充足使用，即```spark.cores.max```应考虑receiver slots。接收器以循环方式分配给执行器。
2. 当从流源接收数据时，接收器创建数据块。每隔blockInterval毫秒生成一个新的数据块。在batchInterval期间创建N个数据块，其中N = batchInterval / blockInterval。这些块由当前执行程序的BlockManager分发给其他执行程序的blockManageer。之后，将在Driver上运行的Network Input Tracker通知块位置以进行进一步处理。 
3. 在Driver上为batchInterval期间创建的块创建RDD。 batchInterval期间生成的块是RDD的分区。每个分区都是spark中的task。 blockInterval == batchinterval意味着创建了一个分区，并且可能在本地处理它。 
4. block中的映射任务在Executors中处理（一个接收块，另一个块复制块），具有块而不管块间隔，除非非本地调度启动。具有更大的blockinterval意味着更大的块。如果```spark.locality.wait```的值很高，增加了在本地节点上处理块的机会。
5. 需要在这两个参数之间找到平衡，以确保在本地处理更大的块。 您可以通过调用```inputDstream.repartition（n）```来定义分区数，而不是依赖于batchInterval和blockInterval。这会随机重新调整RDD中的数据以创建n个分区。是的，为了更大的并行性。虽然以shuffle为代价。 RDD的处理由Driver的jobcheduler作为工作安排。在给定的时间点，只有一个job处于活动状态。因此，如果一个job正在执行，则其他job将排队。 
6. 如果您有两个dstream，将形成两个RDD，并且将创建两个将一个接一个地安排的作业。为了避免这种情况，你可以结合两个dstreams。这将确保为dstream的两个RDD形成单个unionRDD。然后，此unionRDD被视为单个作业。但是，RDD的分区不受影响。
7. 如果批处理时间超过批处理间隔，那么显然接收者的内存将开始填满并最终导致抛出异常（最可能是BlockNotFoundException）。目前，没有办法暂停接收器。使用SparkConf配置```spark.streaming.receiver.maxRate```，可以限制接收器的速率。
