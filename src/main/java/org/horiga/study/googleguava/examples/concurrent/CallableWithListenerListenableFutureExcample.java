package org.horiga.study.googleguava.examples.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class CallableWithListenerListenableFutureExcample {
	
	private static Logger log = LoggerFactory
			.getLogger(CallableWithListenerListenableFutureExcample.class);
	
	protected static ListeningExecutorService es = MoreExecutors.listeningDecorator(
						Executors.newCachedThreadPool(new ThreadFactory() {
									public Thread newThread(Runnable r) {
										return new Thread(r, "task-worker#" + RandomUtils.nextInt(100));
									}}));
	
	protected static ExecutorService th_pool = Executors.newCachedThreadPool(new ThreadFactory() {
				public Thread newThread(Runnable r) {
					return new Thread(r, "thrd-worker#" + RandomUtils.nextInt(100));
				}
			});
	
	public void test() throws Exception {
		
		log.debug(">>> test-task/start");
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		final ListenableFuture<String> future = es.submit(new Callable<String>() {
			/* (non-Javadoc)
			 * @see java.util.concurrent.Callable#call()
			 */
			public String call() throws Exception {
				
				try {
					long waitMillis = Long.parseLong(RandomStringUtils.randomNumeric(5));
					
					log.debug("process waiting.... / waitMillis={}ms", waitMillis);
					
					if (waitMillis > 30000)
						throw new IllegalStateException("system busy...");
					
					log.debug("processing...");
					Thread.sleep(waitMillis); // do anything
					log.debug("process completed.");
					
					return "callback-task finished/" + Thread.currentThread().getName();
					
				} finally {
					latch.countDown();
				}
			}
		});
		
		future.addListener(new Thread() {
			public void run() {
				try {
					log.debug("--- start Listener: isDone={}, isCancelled={}", future.isDone(), future.isCancelled());
					log.debug("Listener# future.get()={}", future.get()); // if task failed future.get returned 'java.util.concurrent.ExecutionException'
					log.debug("--- end Listener");
				} catch (Exception e) {
					log.error("Failed", e);
				}
			}
		}, th_pool);
		
		log.debug(">>> test-task/end");
	}
	
	public static void main(String[] args) {
		try {
			new CallableWithListenerListenableFutureExcample().test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
