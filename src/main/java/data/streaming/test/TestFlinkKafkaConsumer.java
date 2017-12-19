package data.streaming.test;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming._aux.AllWindowFunctionImpl;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class TestFlinkKafkaConsumer {


	public static void main(String... args) throws Exception {

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();


		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(props.getProperty("CLOUDKARAFKA_TOPIC").trim(), 
				new SimpleStringSchema(), props));
		
		AllWindowFunction<String, String, TimeWindow> function = new AllWindowFunctionImpl();
		
		stream.timeWindowAll(Time.seconds(60))
			.apply(function)
			.filter(x -> Utils.isValid(x))
			.map(x -> Utils.createTweetDTO(x)).print(); //Coge toda la información cada 10 segundos y la empaquetas

		//stream.print();
		// TODO 4: Hacer algo mÃ¡s interesante que mostrar por pantalla.

		// execute program
		env.execute("Twitter Streaming Consumer");
	}

}
