package com.sev7e0.wow.core.java;

import org.apache.spark.Partitioner;

/**
 * Title:  PartitionerTestJava.java
 * description: TODO
 *
 * @author sev7e0
 * @version 1.0
 * @since 2020-04-29 22:23
 **/

//在spark中Partitioner为一个抽象类
public class PartitionerTestJava extends Partitioner {

	@Override
	public int numPartitions() {
		return 0;
	}

	@Override
	public int getPartition(Object key) {
		return 0;
	}
}
