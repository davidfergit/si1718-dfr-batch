package data.streaming.test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import data.streaming.stats.Statistics;

public class TestBatch {


	public static void main(String... args) throws Exception {
		
		Statistics runnable = new Statistics();

		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(runnable, 0, 180, TimeUnit.SECONDS);
		
	}

}
