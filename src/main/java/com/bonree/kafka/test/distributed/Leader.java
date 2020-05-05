package com.bonree.kafka.test.distributed;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bonree.kafka.test.Main;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Leader {
	private final CuratorFramework client;
	private final LeaderSelector selector;
	private final List<String> tasks;
	private PathChildrenCache discoverPachChildren;
	private PathChildrenCache taskPathChildren;
	private ScheduledExecutorService es;
	/**
	 * 	保存任务结果信息, key为hostname, value为已做完任务的woker信息
	 */
	private final Map<String, List<Result>> taskResult;
	/**
	 * 	存活的wokers信息, key为hostname, value为woker对象
	 */
	private final Map<String, WorkerInfo> wokers;
	/**
	 * 	任务执行索引
	 */
	private int taskIndex = 0;
	private boolean started = false;
	
	public Leader(CuratorFramework client, LeaderSelector selector, List<String> tasks) {
		this.client = client;
		this.selector = selector;
		this.tasks = tasks;
		this.taskResult = new ConcurrentHashMap<>();
		this.wokers = new ConcurrentHashMap<>();
	}
	
	public void start() throws Exception {
		selector.registerWorkerListener(new LeaderListener() {
			@Override
			public void stopLeader() {
				try {
					notLeader();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			@Override
			public void becomeLeader() {
				try {
					Leader.this.becomeLeader();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void stop() throws IOException {
		notLeader();
		selector.unregisterListener();
	}
	
	private void becomeLeader() throws Exception {
		this.discoverPachChildren = new PathChildrenCache(client, WorkerMonitor.DISCOVER_PATH, true);
		//给woker的服务发现节点加上状态变化监听器
		discoverPachChildren.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					switch(event.getType()) {
							case CHILD_ADDED:
								WorkerInfo info = parseWorker(event.getData());
								System.out.println("add worker: " + info);
								wokers.put(info.getHostname(), info);
								break;
							case CHILD_REMOVED:
								info = parseWorker(event.getData());
								System.out.println("remove worker: " + info);
								wokers.remove(info.getHostname());
								break;
							default:
							break;
					}
				}
			});
		discoverPachChildren.start();
		//读取已经发布的woker
		discoverPachChildren.getCurrentData()
		.stream()
		.map((childData) -> parseWorker(childData))
		.forEach((worker) -> wokers.put(worker.getHostname(), worker));
		System.out.println("current wokers: " + wokers);
		
		//删除已经发布的task结果信息, 防止影响本次leader任务执行
		if(client.checkExists().forPath(WorkerMonitor.TASK_PATH) != null) {
			client.delete().deletingChildrenIfNeeded().forPath(WorkerMonitor.TASK_PATH);
		}
		
		this.taskPathChildren = new PathChildrenCache(client, WorkerMonitor.TASK_PATH, true);
		//task结果信息节点加上监听器
		taskPathChildren.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				 if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
					 String path = event.getData().getPath();
					 String data = new String(client.getData().forPath(path), StandardCharsets.UTF_8);
					 
					 String nodeName = ZKPaths.getNodeFromPath(path);
					 String[] args = nodeName.split("_");
					 String taskId = args[0];
					 
					 Result result = JSON.parseObject(data, Result.class);
					 taskResult.computeIfAbsent(taskId, (k) -> new ArrayList<Result>()).add(result);
					 
					 //清除已经收到的信息,防止临时节点过多
					 client.delete().forPath(path);
				 }
			}
		});
		taskPathChildren.start();
		
		es = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
		es.scheduleAtFixedRate(manager(), 0, 5, TimeUnit.SECONDS);
	}
	
	private void notLeader() throws IOException {
		discoverPachChildren.close();
		discoverPachChildren = null;
		wokers.clear();
		
		taskPathChildren.close();
		taskPathChildren = null;
		
		es.shutdown();
		es = null;
	}
	
	private Runnable manager() {
		return () -> {
			try {
				loop();
			}catch(Exception e) {
				e.printStackTrace();
			}
		};
	}
	
	@VisibleForTesting
	void loop() {
		if(taskIndex == tasks.size()) {
			System.out.println("All tasks completed");
			try {
				stop();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		JSONObject task = JSON.parseObject(tasks.get(taskIndex));
		String taskId = task.getString("index");
		int threadCount = task.getIntValue("threadCount");
		
		//判断上个任务是否完成
		if(started) {
			List<Result> completed = taskResult.get(taskId);
			if(completed == null) {
				System.out.println(String.format("waitting task: %s", taskId));
				return;
			}
			int completedCount = completed.stream().mapToInt(Result::getCompleteThread).sum();
			if(completedCount < threadCount) {
				System.out.println(String.format("waitting task: %s total count: %s, completed count: %s", taskId, threadCount, completedCount));
				return;
			}
			
			List<Object[]> result = new ArrayList<>();
			for(Result r : completed) {
				result.add(r.getRecords().toArray());
			}
			Object[] metrics = Main.calcTotal(result, 6, 3);
			
			Object[] records = new Object[9];
			records[0] = taskId;
			records[1] = threadCount;
			//参数
			System.arraycopy(result.get(0), 2, records, 0, 4);
			//统计值
			System.arraycopy(metrics, 0, records, 6, 3);
			
			String fileName = task.getString("fileName");
			try {
				Main.createCSVFile(Main.HEADERS, records, fileName);
				//任务已完成
				taskIndex++;
				if(started) {
					started = false;
				}
				return;
			} catch (IOException e) {
				System.out.println(String.format("write records to file[%s] error! msg: %s", fileName, e.getMessage()));
			}
		}
		
		int capacity = wokers.values().stream().mapToInt(WorkerInfo::getCapacity).sum();
		
		if(capacity < threadCount) {
			System.out.println(String.format("task[%s] need %s capacity but get %s. Please add worker", taskId, threadCount, capacity));
			return;
		}
		//存储分配规则
		Map<String, Integer> taskAssign = new HashMap<>();
		List<WorkerInfo> wokerInfos = new ArrayList<>(wokers.values());
		//任务分配,每个woker均匀分配
		int index = 0;
		for(int i = 0; i < threadCount; i++) {
			WorkerInfo info = wokerInfos.get(index++ % wokerInfos.size());
			int count = taskAssign.getOrDefault(info.getHostname(), 0);
			
			//当前woker容量已满后分配给下一个
			while(count + 1 > info.getCapacity()) {
				info = wokerInfos.get(index++ % wokerInfos.size());
				count = taskAssign.getOrDefault(info.getHostname(), 1);
			}
			taskAssign.put(info.getHostname(), count + 1);
		}
		System.out.println(String.format("taskId: %s, script: %s, thread: %s, assign: %s", taskId, task.getJSONObject("command"), threadCount, taskAssign));
		
		JSONObject cloneTask = (JSONObject) task.clone();
		CuratorTransaction transaction = client.inTransaction();
		CuratorTransactionBridge bridge = null;
		try {
			for(Entry<String, Integer> entry : taskAssign.entrySet()) {
				cloneTask.put("threadCount", entry.getValue());
				String data = cloneTask.toJSONString();
				if(bridge == null) {
					bridge = transaction.create()
					.withMode(CreateMode.EPHEMERAL)
					.forPath(WorkerMonitor.WORKER_PATH + '/' + entry.getKey() + '/' + taskId,
							data.getBytes(StandardCharsets.UTF_8)
					);
				}else {
					bridge.and().create().withMode(CreateMode.EPHEMERAL)
					.forPath(WorkerMonitor.WORKER_PATH + '/' + entry.getKey() + '/' + taskId,
							data.getBytes(StandardCharsets.UTF_8)
					);
				}
			}
			bridge.and().commit();
			
			if(!started) {
				started = true;
			}
		} catch (Exception e) {
			System.err.println(String.format("taskId[%s] assign task to woker[%s] error! msg: %s", taskId, taskAssign, e.getMessage()));
		}
	}
	
	private WorkerInfo parseWorker(ChildData childData) {
		String path = childData.getPath();
		WorkerInfo info = new WorkerInfo();
		info.setHostname(path.substring(path.lastIndexOf('/') + 1));
		info.setCapacity(Integer.parseInt(new String(childData.getData())));
		return info;
	}
	
}
