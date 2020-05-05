package com.bonree.kafka.test.distributed;

import java.nio.charset.StandardCharsets;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkerMonitorTest {
	static CuratorFramework client;

	@BeforeClass
	public static void  before() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		client = CuratorFrameworkFactory.newClient("192.168.101.106:2181", retryPolicy);
		client.start();
	}

	@Test
	public void testWorker() throws Exception {
		String hostname = "localhost";
		
		WorkerMonitor worker = new WorkerMonitor(client, new WorkerInfo(hostname, 1));
		worker.registerWorkerListener(new LeaderTest.FakeWokerListener(worker));
		
		Thread.sleep(1000);
		
		Stat stat =	client.checkExists().forPath(WorkerMonitor.WORKER_PATH);
		String workerData = new String(client.getData().forPath(WorkerMonitor.WORKER_PATH));
		System.out.println("stat: " + stat + " workerData: " + workerData);
		if(stat != null) {
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath(WorkerMonitor.WORKER_PATH + "/localhost/test", "{\"taskCount\":2}".getBytes(StandardCharsets.UTF_8));
			
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath(WorkerMonitor.WORKER_PATH + "/localhost/test2", "{\"taskCount\":2}".getBytes(StandardCharsets.UTF_8));
		}
		Thread.sleep(20000);
		
		System.out.println(client.getChildren().forPath(WorkerMonitor.TASK_PATH));
	}

	@AfterClass
	public static void after() {
		client.close();
	}
}
