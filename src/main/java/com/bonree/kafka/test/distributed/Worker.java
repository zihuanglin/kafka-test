package com.bonree.kafka.test.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bonree.kafka.test.Config;
import com.bonree.kafka.test.Main;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 	任务执行节点
 * @author Administrator
 *
 */
public class Worker {
	private final String[] envp;
	private final WorkerMonitor monitor;
	private ExecutorService exec;
	private ExecutorService taskExecutor;
	
	public Worker(Config config, WorkerMonitor monitor) {
		 this.envp = getSystemEnv(config);
		 this.monitor = monitor;
		 exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
	}
	
	public void start() throws Exception {
		//创建最多的线程
		taskExecutor = Executors.newFixedThreadPool(
				monitor.getInfo().getCapacity(),
				new ThreadFactoryBuilder().setDaemon(true).build());
		
		monitor.registerWorkerListener((taskId, task) -> 
				exec.submit(
					() -> {
						try {
							JSONObject object = JSON.parseObject(task);
							int taskCount = object.getIntValue("threadCount");
							JSONObject commands = object.getJSONObject("command");
							
							String script = commands.getString("script");
							int numRecords = commands.getIntValue("num-records");
							int throughput = commands.getIntValue("throughput");
							String topic = commands.getString("topic");
							String payloadFile = commands.getString("payload-file");
							
							JSONObject props = commands.getJSONObject("producer-props");
							String bootstrapServers = props.getString("bootstrap.servers");
							int batchSize = props.getIntValue("batch.size");
							int lingerMs = props.getIntValue("linger.ms");
							int bufferMemory = props.getIntValue("buffer.memory");
							String compressionType = props.getString("compression.type");
							
							String command = String.format("%s --num-records %s --throughput %s --topic %s --payload-file %s "
									+ "--producer-props bootstrap.servers=%s batch.size=%s linger.ms=%s buffer.memory=%s compression.type=%s",
									script,
									numRecords,
									throughput,
									topic,
									payloadFile,
									bootstrapServers,
									batchSize,
									lingerMs,
									bufferMemory,
									compressionType
									);
							System.out.println(String.format("thread: %s, exec command: %s", taskCount, command));
							
							//提交任务
							List<Future<Object[]>> futures = new ArrayList<>();
							for(int i = 0; i < taskCount; i++) {
								futures.add(taskExecutor.submit(() -> startScript(command, envp)));
							}
							//拼接csv数据
							List<Object> records = new ArrayList<>();
							records.add(taskId);
							records.add(taskCount);
							records.add(batchSize);
							records.add(lingerMs);
							records.add(bufferMemory);
							records.add(compressionType);
							
							List<Object[]> results = new ArrayList<>();
							for(int i = 0; i < taskCount; i++) {
								results.add(futures.get(i).get());
							}
							//统计性能指标
							records.addAll(Arrays.asList(Main.calcTotal(results, 0, 3)));
							
							Result result = new Result();
							result.setCompleteThread(taskCount);
							result.setRecords(records);
							monitor.announceTask(taskId, JSON.toJSONString(result));
						}catch(Exception e) {
							e.printStackTrace();
						}
				 }
			)
		);
		System.out.println("woker started capacity:" + monitor.getInfo().getCapacity());
	}

	public void stop() throws IOException {
		monitor.unregisterListener();
		taskExecutor.shutdown();
		System.out.println("woker shutdown");
	}

	/**
	 * 获取系统变量, 写入字符串数组中, 并设置kafka脚本执行内存
	 * @param envp
	 * @return
	 */
	private static String[] getSystemEnv(Config config) {
		String[] envp = new String[System.getenv().size() + 1];
		int envpIndex = 0;
		for(Entry<String, String> entry : System.getenv().entrySet()) {
			envp[envpIndex++] = String.format("%s=%s", entry.getKey(), entry.getValue());
		}
		envp[envpIndex++] = "KAFKA_HEAP_OPTS=" + config.getKafkaHeapOpts();
		return envp;
	}

	public static Object[] startScript(String command, String... envp) throws IOException, InterruptedException {
		Process process = Runtime.getRuntime().exec(command, envp);
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		
		String last = null;
		for(String line; (line = reader.readLine()) != null;) {
			last = line;
			System.out.println(line);
		}
		
		final int statusCode = process.waitFor();
		if(statusCode == 0) {
			return parseLine(last);
		}else {
			throw new RuntimeException("Process exit value is not normal termination. statusCode: " + statusCode);
		}
	}

	/**
	 * 解析kafka-producer-perf-test.sh脚本输出的日志
	 * @param line 最终输出的总和统计信息
	 * @return
	 */
	public static String[] parseLine(String line) {
		String[] args = line.split(",");
		String arg1 = args[1].replaceAll("records/sec", "").replaceAll("\\(", "").replaceAll("MB/sec\\)", "").trim();
		String[] arg1s = arg1.split("  ");
		
		String arg7 = args[7].replaceAll("ms 99.9th.", "").trim();
		return new String[] {arg1s[0], arg1s[1], arg7};
	}

}
