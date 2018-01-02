package data.streaming.stats;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import data.streaming._aux.ValidTagsTweetEndpoIntinitializer;
import data.streaming.mongo.MongoKeywords;
import data.streaming.utils.LoggingFactory;

public class ProducerJob implements org.quartz.Job{
	
	private static final Integer PARALLELISM = 2;

	public ProducerJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.err.println("Hello World!  ProducerJob is executing.");
        
        for (String s: MongoKeywords.getKeywords()) {
    		System.out.println(s);
    	}
    	
    	System.out.println("RECALCULATED KEYWORDS");
    	System.out.println("*******************************************************");

		TwitterSource twitterSource = null;
		try {
			twitterSource = new TwitterSource(LoggingFactory.getTwitterCredentias());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Establecemos el filtro
		twitterSource.setCustomEndpointInitializer(new ValidTagsTweetEndpoIntinitializer(MongoKeywords.getKeywords()));

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(PARALLELISM);
		
		/*env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
		  3, // number of restart attempts
		  Time.of(120, TimeUnit.SECONDS) // delay
		));*/

		// Añadimos la fuente y generamos el stream como la salida de las llamadas
		// asíncronas para salvar los datos en MongoDB
		DataStream<String> stream = env.addSource(twitterSource);

		Properties props = null;
		try {
			props = LoggingFactory.getCloudKarafkaCredentials();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration<String> config = FlinkKafkaProducer010
				.writeToKafkaWithTimestamps(stream, props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(),
						props);
		config.setWriteTimestampToKafka(false);
		config.setLogFailuresOnly(false);
		config.setFlushOnCheckpoint(true);

		stream.print();

		try {
			env.execute("Twitter Streaming Producer");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
}
