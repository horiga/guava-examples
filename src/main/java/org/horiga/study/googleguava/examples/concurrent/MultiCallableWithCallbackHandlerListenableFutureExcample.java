package org.horiga.study.googleguava.examples.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class MultiCallableWithCallbackHandlerListenableFutureExcample {

	private static Logger log = LoggerFactory
			.getLogger(MultiCallableWithCallbackHandlerListenableFutureExcample.class);

	protected static ListeningExecutorService les = MoreExecutors
			.listeningDecorator(Executors
					.newCachedThreadPool(new ThreadFactory() {
						public Thread newThread(Runnable r) {
							return new Thread(r, "task-worker#"
									+ RandomUtils.nextInt(100));
						}
					}));

	protected static ExecutorService callbackHandlers = Executors
			.newCachedThreadPool(new ThreadFactory() {
				public Thread newThread(Runnable r) {
					return new Thread(r, "cab-handler#"
							+ RandomUtils.nextInt(100));
				}
			});

	public void test() throws Exception {
		log.debug(">>> test-task/start");
		try {
			final int taskCount = 5;
			final CountDownLatch latch = new CountDownLatch(taskCount);
			final CountDownLatch startGate = new CountDownLatch(1);
			
			for (int i = 0; i < taskCount; i++) {
				// invoked task
				Futures.addCallback(les.submit(new Callable<String>() {
					
					public String call() throws Exception {
						
						try {
							startGate.await(1000L, TimeUnit.MILLISECONDS);
							
							long waitMillis = Long.parseLong(RandomStringUtils.randomNumeric(5));
							
							log.debug("process waiting.... / waitMillis={}ms", waitMillis);
							
							if (waitMillis > 50000)
								throw new IllegalStateException("system busy...");
							
							log.debug("processing...");
							Thread.sleep(waitMillis); // do anything
							log.debug("process completed.");
							
							return "callback-task finished/" + Thread.currentThread().getName();
							
						} finally {
							latch.countDown();
						}
					}
				}),
				new FutureCallback<String>() {
							public void onSuccess(String result) {
								log.info("callback process success. Unresolved task count={},  {}", latch.getCount(), result);
								if (latch.getCount() <= 0) {
									log.info("#### ALL TASK FINISHED ####");
								}
							}
							public void onFailure(Throwable t) {
								log.warn("callback process failed. Unresolved task count={} : {}", latch.getCount(), t.getMessage());
								if (latch.getCount() <= 0) {
									log.info("#### ALL TASK FINISHED ####");
								}
							}
						}, 
				callbackHandlers);
			}
			
			// start all tasks
			startGate.countDown();

		} catch (Exception e) {
			log.error("Failed", e);
		}
		log.debug(">>> test-task/end");
	}

	public static void main(String[] args) {
		try {
			new MultiCallableWithCallbackHandlerListenableFutureExcample()
					.test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
