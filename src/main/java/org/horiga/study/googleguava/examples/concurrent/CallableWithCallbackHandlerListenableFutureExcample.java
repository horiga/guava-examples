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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class CallableWithCallbackHandlerListenableFutureExcample {
	
	private static Logger log = LoggerFactory
			.getLogger(CallableWithCallbackHandlerListenableFutureExcample.class);
	
	private static ListeningExecutorService es = MoreExecutors.listeningDecorator(
			Executors.newCachedThreadPool(new ThreadFactory() {
						public Thread newThread(Runnable r) {
							return new Thread(r, "task-worker#" + RandomUtils.nextInt(100));
						}}));
	
	private static ExecutorService callbackHandlers = Executors.newCachedThreadPool(new ThreadFactory() {
		public Thread newThread(Runnable r) {
			return new Thread(r, "cab-handler#"
					+ RandomUtils.nextInt(100));
		}
	});
	
	public void test() {
		
		log.debug(">>> test-task/start");
		
		final CountDownLatch latch = new CountDownLatch(1);
		ListenableFuture<String> future = es.submit(new Callable<String>() {
					/* (non-Javadoc)
					 * @see java.util.concurrent.Callable#call()
					 */
					public String call() throws Exception {
						
						try {
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
		});
		Futures.addCallback(future, new FutureCallback<String>() {
			public void onSuccess(String result) {
				log.info("callback process success. {}", result);
			}
			public void onFailure(Throwable t) {
				log.warn("callback process failed.", t);
			}
		}, callbackHandlers);
		
		log.debug(">>> test-task/end");
	}
	
	public static void main(String[] args) {
		try {
			new CallableWithCallbackHandlerListenableFutureExcample().test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
