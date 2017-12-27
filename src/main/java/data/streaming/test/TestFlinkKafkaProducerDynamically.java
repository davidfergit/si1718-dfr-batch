
package data.streaming.test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import data.streaming.stats.ProducerDynamically;

public class TestFlinkKafkaProducerDynamically {

	public static void main(String... args) throws Exception {

		ProducerDynamically runnable = new ProducerDynamically();

		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(runnable, 0, 120, TimeUnit.SECONDS);
		
	}

}
