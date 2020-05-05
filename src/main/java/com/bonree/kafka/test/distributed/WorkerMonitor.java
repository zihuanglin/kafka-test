package com.bonree.kafka.test.distributed;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

/**
 * 	在zk上面创建三个永久节点 worker, task 和discover <br>
 *  woker子节点下面会创建永久的woker主机名称节点, 主机节点下面存放待执行任务信息 <br>
 *  discover子节点下面会创建临时的主机名称并存放任务容量信息, 表示当前woker存活<br>
 * 
 */
public class WorkerMonitor {
	public final static String WORKER_PATH = "/kafka-test/worker";
	public final static String TASK_PATH = "/kafka-test/task";
	public final static String DISCOVER_PATH = "/kafka-test/discover";
	public final static String STATUS_PATH = "/kafka-test/status";
	
	private final CuratorFramework client;
	private final WorkerInfo info;
	private List<WokerListener> listeners = new ArrayList<>();
	private PathChildrenCache pathChildrenCache;
	private boolean started;
	
	public WorkerMonitor(CuratorFramework client, WorkerInfo info) {
		this.client = client;
		this.info = info;
	}
	
	private void start() throws Exception {
		//创建woker节点
		String workerPath = WORKER_PATH + "/" + info.getHostname();
		if(client.checkExists().forPath(workerPath) == null) {
			client
			.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.PERSISTENT)
			.forPath(workerPath);
		}
		//服务发现
		String discoverPath = DISCOVER_PATH + "/" + info.getHostname();
		while(null != client.checkExists().forPath(discoverPath)) {
			Thread.sleep(5000);
			System.out.println("service path is exist, waiting 5s...");
		}
		
		client
		.create()
		.creatingParentsIfNeeded()
		.withMode(CreateMode.EPHEMERAL)
		.forPath(discoverPath, String.valueOf(info.getCapacity()).getBytes(StandardCharsets.UTF_8));
		
		this.pathChildrenCache = new PathChildrenCache(client, workerPath, false);
		pathChildrenCache.start();
		pathChildrenCache.getListenable().addListener(
	        new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					 if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
						 String path = event.getData().getPath();
						 String data = new String(client.getData().forPath(path), StandardCharsets.UTF_8);
						 
						 String taskId = ZKPaths.getNodeFromPath(path);
						 listeners.stream().forEach((listener) -> listener.runTask(taskId, data));
					 }
				}
	        }
		);
		started = true;
	}
	
	public void announceTask(String taskId, String value) {
		try {
			client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath(TASK_PATH + '/' + taskId + '_' + info.getHostname(), value.getBytes(StandardCharsets.UTF_8));
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public synchronized void registerWorkerListener(WokerListener listener) throws Exception {
		if(!started) {
			start();
		}
		
		listeners.add(listener);
	}
	
	public void unregisterListener() throws IOException {
		if(started) {
			started = false;
		}
		if(pathChildrenCache != null) {
			pathChildrenCache.close();
			pathChildrenCache = null;
		}
		listeners.clear();
	}

	public WorkerInfo getInfo() {
		return info;
	}
}
