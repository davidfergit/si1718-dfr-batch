package data.streaming.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.fasterxml.jackson.databind.ObjectMapper;

import data.streaming.dto.TweetDTO;

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

}
