package com.bonree.kafka.test.distributed;

public interface WokerListener {
	void runTask(String taskId, String task);
}
