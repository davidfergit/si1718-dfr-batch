package data.streaming.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;

import com.fasterxml.jackson.databind.ObjectMapper;

import data.streaming.dto.KeywordDTO;
import data.streaming.dto.TweetDTO;
import data.streaming.mongo.MongoResearchersRating;

public class Utils {
	
	
	public static final String[] TAGNAMES = { "#TheWalkingDeadUK" };
	private static final ObjectMapper mapper = new ObjectMapper();
	
	public static Date getTwitterDate(String date) throws ParseException {
	  SimpleDateFormat dateFormat = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
	  dateFormat.setLenient(false);
	  return dateFormat.parse(date);
	}

	public static TweetDTO createTweetDTO(String x) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {

		}
		return result;
	}
	
	public static Boolean isValid(String x) {
		Boolean result = true;

		try {
			mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {
			result = false;
		}
		return result;
	}
	
	private static Collection<? extends Event> createEventCollection() {
		List<Event> result = new LinkedList<>();
		
		/* Obtengo las keywords con su correspondiente rating */
		Iterable<org.bson.Document> ratings = MongoResearchersRating.getResearchersRatingCollection().find();
		
		for (org.bson.Document dto: ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getString("firstResearcher").hashCode());
			r.setUserId(dto.getString("secondResearcher").hashCode());
			r.setRating(dto.getDouble("rating"));
			result.add(r);
		}

		return result;
	}

}
