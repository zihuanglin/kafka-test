package com.bonree.kafka.test.distributed;

public interface LeaderListener {

	void becomeLeader();
	
	void stopLeader();
}
