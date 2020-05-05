package com.bonree.kafka.test.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

public class LeaderSelector {
	private final static String LATCH_PATH = "/kafka-test/leader";
	private List<LeaderListener> listeners = new ArrayList<>();
	boolean started = false;
	private LeaderLatch newLeaderLatch;
	private final CuratorFramework curator;
	private final String hostname;
	
	public LeaderSelector(CuratorFramework curator, String hostname) {
		this.curator = curator;
		this.hostname = hostname;
	}
	
	private void start() {
		started = true;
		newLeaderLatch = new LeaderLatch(
		        curator, LATCH_PATH, hostname
		    );
		newLeaderLatch.addListener(new LeaderLatchListener() {
			@Override
			public void notLeader() {
				System.out.println("not leader");
				listeners.stream().forEach(LeaderListener::stopLeader);
			}
			
			@Override
			public void isLeader() {
				System.out.println("is leader");
				listeners.stream().forEach(LeaderListener::becomeLeader);
			}
		});
		
		try {
			newLeaderLatch.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void unregisterListener() throws IOException {
		if(started) {
			started = false;
		}
		if(newLeaderLatch != null) {
			newLeaderLatch.close();
			newLeaderLatch = null;
		}
		listeners.clear();
	}
	
	public synchronized void registerWorkerListener(LeaderListener listener) {
		if(!started) {
			start();
		}
		
		listeners.add(listener);
	}
}
