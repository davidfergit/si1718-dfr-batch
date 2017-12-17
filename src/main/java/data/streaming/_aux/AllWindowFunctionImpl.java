package data.streaming._aux;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import data.streaming.dto.TweetDTO;
import data.streaming.mongo.MongoKeywords;
import data.streaming.utils.Utils;

public class AllWindowFunctionImpl implements AllWindowFunction<String, String, TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8867153038674796082L;

	@Override
	public void apply(TimeWindow arg0, Iterable<String> arg1, Collector<String> arg2) throws Exception {
		// TODO Auto-generated method stub
		
		List<org.bson.Document> tweets = new ArrayList<org.bson.Document>();
		
		for(String s:arg1) {
			if (Utils.isValid(s)) {
				TweetDTO aux = Utils.createTweetDTO(s);
				
				//Convierto la fecha de Twitter a Date
				Date dateAux = Utils.getTwitterDate(aux.getCreatedAt());
				
				//Formato dd/MM/yyyy
				DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		        String date = formatter.format(dateAux);
				
		        //Almaceno la fecha formateada
				aux.setCreatedAt(date);
				tweets.add(MongoKeywords.convertTweetDTOToDocument(aux));
			}
			//arg2.collect(s);
		}
		
		if (!tweets.isEmpty()) {
			MongoKeywords.saveTweetsFilteredByKeywords(tweets);
		}
	}

}
