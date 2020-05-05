package com.bonree.kafka.test;

import org.junit.Test;

public class SplitTest {

	@Test
	public void test() {
		
		String task = "1,2,/data/br/base/kafka_2.12-2.5.0/bin/kafka-producer-perf-test.sh --num-records 10000000 --throughput 1000000 --topic test-partition-6 --payload-file sdk.data.10w,bootstrap.servers=B-197:9096,B-198:9096 batch.size=81920 linger.ms=10 buffer.memory=67108864 compression.type=zstd";
		
		for(String s : task.split(",")) {
			System.out.println(s);
		}
	}
}
