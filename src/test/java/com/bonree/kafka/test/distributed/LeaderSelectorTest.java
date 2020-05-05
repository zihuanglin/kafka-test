package com.bonree.kafka.test.distributed;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderSelectorTest {
	private static CuratorFramework client;

	@BeforeClass
	public static void init() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		
		client = CuratorFrameworkFactory.newClient("192.168.101.106:2181", retryPolicy);
		client.start();
	}

	@Test
	public void testSelector() throws InterruptedException, IOException {
		class FakeLeaderListener implements LeaderListener{
			boolean leader;
			@Override
			public void becomeLeader() {
				leader = true;
			}

			@Override
			public void stopLeader() {
				leader = false;				
			}
		}
		FakeLeaderListener listener1 = new FakeLeaderListener();
		FakeLeaderListener listener2 = new FakeLeaderListener();
		
		LeaderSelector selector = new LeaderSelector(client, "master");
		LeaderSelector selector2 = new LeaderSelector(client, "node1");
		
		selector.registerWorkerListener(listener1);
		Thread.sleep(5000);
		assertTrue(listener1.leader);
		selector.unregisterListener();
		
		selector2.registerWorkerListener(listener2);
		Thread.sleep(5000);
		assertTrue(listener2.leader);
		selector2.unregisterListener();
	}

	@AfterClass
	public static void stop() {
		client.close();
	}
}
