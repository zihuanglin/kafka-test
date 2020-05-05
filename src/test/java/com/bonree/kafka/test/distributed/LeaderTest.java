package com.bonree.kafka.test.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class LeaderTest {
private static CuratorFramework client;
	public static final String task = "{\"taskCount\":2}";
	@BeforeClass
	public static void init() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		
		client = CuratorFrameworkFactory.newClient("192.168.101.106:2181", retryPolicy);
		client.start();
	}
	public static class FakeWokerListener implements  WokerListener{
		WorkerMonitor woker;
		public FakeWokerListener(WorkerMonitor woker) {
			this.woker = woker;
		}
		@Override
		public void runTask(String taskId, String task) {
			JSONObject object = JSON.parseObject(task);
			int taskCount = object.getIntValue("taskCount");
			woker.announceTask(taskId, JSON.toJSONString(getResult(taskCount)));
		}
	}
	
	@Test
	public void test() throws Exception {
		
		WorkerMonitor woker1 = new WorkerMonitor(client, new WorkerInfo("test-worker", 1));
		woker1.registerWorkerListener(new FakeWokerListener(woker1));
		
		List<String> tasks = new ArrayList<>();
		tasks.add(" {\"index\":1,\"taskCount\":1}");
		tasks.add("{\"index\":2,\"taskCount\":4}");
		tasks.add("{\"index\":3,\"taskCount\":3}");
		tasks.add("{\"index\":4,\"taskCount\":5}");
		
		LeaderSelector selector = new LeaderSelector(client, "master");
		Leader leader = new Leader(client, selector, tasks);
		leader.start();
		
		WorkerMonitor woker2 = new WorkerMonitor(client, new WorkerInfo("test-worker2", 2));
		woker2.registerWorkerListener(new FakeWokerListener(woker2));
		
		WorkerMonitor woker3 = new WorkerMonitor(client, new WorkerInfo("test-worker3", 2));
		woker3.registerWorkerListener(new FakeWokerListener(woker3));
		
		Thread.sleep(25000);
		leader.stop();
	}
	
	private static Result getResult(int count) {
		Result result = new Result();
		result.setCompleteThread(count);
		result.setRecords(Arrays.asList("10", "10", "1"));
		return result;
	}
	
	@AfterClass
	public static void stop() {
		client.close();
	}
}
