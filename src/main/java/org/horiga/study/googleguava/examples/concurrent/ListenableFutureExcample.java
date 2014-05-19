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

public class ListenableFutureExcample {
	
	private static Logger log = LoggerFactory
			.getLogger(ListenableFutureExcample.class);
	
	private static ListeningExecutorService es = MoreExecutors.listeningDecorator(
						Executors.newCachedThreadPool(new ThreadFactory() {
									public Thread newThread(Runnable r) {
										return new Thread(r, "les-worker-" + RandomUtils.nextInt());
									}}));
	
	private ExecutorService callbackHandlers = Executors.newCachedThreadPool(new ThreadFactory() {
				public Thread newThread(Runnable r) {
					return new Thread(r, "callback-handler-"
							+ RandomUtils.nextInt());
				}
			});

	private ExecutorService th_pool = Executors.newCachedThreadPool(new ThreadFactory() {
				public Thread newThread(Runnable r) {
					return new Thread(r, "th-worker-" + RandomUtils.nextInt());
				}
			});
	
	static class AbstractTask {
		
		private final CountDownLatch latch;
		
		public AbstractTask(CountDownLatch latch) { this.latch = latch; }
		
		protected void process() throws Exception {
			try {
				long waitMillis = Long.parseLong(RandomStringUtils
						.randomNumeric(5));
				log.debug("process waiting.... / waitMillis={}ms", waitMillis);
				Thread.sleep(waitMillis);
				log.debug("process completed....");
			} finally {
				latch.countDown();
			}
		}
	}
	
	static class CallableTask extends AbstractTask implements Callable<String> {
		
		public CallableTask(CountDownLatch latch) {
			super(latch);
		}
		
		public String call() throws Exception {
			process();
			return "callback-task finished/" + Thread.currentThread().getName();
		}
		
	}
	
	static class RunnableTask extends AbstractTask implements Runnable {
		
		public RunnableTask(CountDownLatch latch) {
			super(latch);
		}
		
		public void run() {
			try {
				process();
			} catch (Exception e) {
				log.error("runnable-task failed.", e);
			}
		}
	}
	
	public void testAtCallableWithCallbackHandler() {
		
		log.debug(">>> test-task / testAtCallableWithCallbackHandler : start");
		final CountDownLatch latch = new CountDownLatch(1);
		ListenableFuture<String> future = es.submit(new CallableTask(latch));
		Futures.addCallback(future, new FutureCallback<String>() {
			public void onSuccess(String result) {
				log.info("callback process success. {}", result);
			}

			public void onFailure(Throwable t) {
				log.warn("callback process failed.", t);
			}
		}, callbackHandlers);
		log.debug(">>> test-task / testAtCallableWithCallbackHandler");
	}
	
	public void testAtCallableWithListener() throws Exception {
		
		log.debug(">>> test-task / testAtCallableWithListener : start");
		
		final ListenableFuture<String> future = es.submit(new CallableTask(new CountDownLatch(1)));
		
		future.addListener(new Thread() {
			public void run() {
				try {
					log.debug("--- start Listener");
					log.debug("Listener# future.get()={}", future.get());
					log.debug("--- end Listener");
				} catch (Exception e) {
					log.error("Failed", e);
				}
				
			}
		}, th_pool);
		
		log.debug(">>> test-task / testAtCallableWithListener : end");
	}
	
	
	public void testAtRunnableWithListener() throws Exception {
		
		log.debug(">>> test-task / testAtRunnableWithListener : start");
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		// ListenableFuture<String> future = es.submit(new RunnableTask(latch)); /* Notes: Not available compile. */
		final ListenableFuture<?> f = es.submit(new RunnableTask(latch));
		
		/*
		 * 'com.google.common.util.concurrent.Futures#addCallback' only supported 'java.util.concurrent.Callback<V>' 
		 */
		/*
		Futures.addCallback(future, new FutureCallback<String>() {
			public void onSuccess(String result) {
				log.info("callback process success. {}", result);
			}

			public void onFailure(Throwable t) {
				log.warn("callback process failed.", t);
			}
		}, callbackHandlers);
		*/
		
		f.addListener(new Thread() {
			public void run() {
				try {
					// もちろん java.util.Runnable で実行しているため、ListenableFuture#get() は何も帰ってこない(null) 
					log.debug("*** (start)ListenableFuture-listener1 : future.get()={}", f.get());
					Thread.sleep(1000L);
					log.debug("*** (end)ListenableFuture-listener1");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, th_pool);
		
		f.addListener(new Thread() {
			public void run() {
				try {
					// もちろん java.util.Runnable で実行しているため、ListenableFuture#get() は何も帰ってこない(null) 
					log.debug("*** (start)ListenableFuture-listener2 : f.get()={}", f.get());
					Thread.sleep(100L);
					log.debug("*** (end)ListenableFuture-listener2");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, th_pool);
		
		
	}
	
	
	public void testAtMultiCallableWithListener() throws Exception {
		log.debug(">>> test-task / testAtMultiCallableWithListener");
		try {
			final int taskCount= 5;
			final CountDownLatch latch = new CountDownLatch(taskCount);
			
			for (int i=0; i<taskCount; i++) {
				final ListenableFuture<String> future = es.submit(new CallableTask(latch));
				future.addListener(new Runnable() {
					public void run() {
						try {
							log.info("future.get()={}, latch.getCount()={}", future.get(), latch.getCount());
							
							if (latch.getCount() <= 0) {
								log.info("#### ALL TASK FINISHED ####");
							}
							
						} catch (Exception e) {
							log.error("Failed", e);
						}
					}
				}, th_pool);
			}
			
		} catch (Exception e) {
			log.error("Failed", e);
		}
		log.debug(">>> test-task / testAtMultiCallableWithListener");
	}
	
	
	public static void main(String[] args) {
		try {
			//new ListenableFutureExcample().testAtCallbackWithCallbackHandler();
			//new ListenableFutureExcample().testAtCallableWithListener();
			//new ListenableFutureExcample().testAtRunnableWithListener();
			new ListenableFutureExcample().testAtMultiCallableWithListener();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
