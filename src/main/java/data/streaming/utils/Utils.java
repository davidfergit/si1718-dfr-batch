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

import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;

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
	
	public static ItemRecommender getRecommender(Set<KeywordDTO> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection());

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);
		// config.bind(BaselineScorer.class,
		// ItemScorer.class).to(UserMeanItemScorer.class);
		// config.bind(UserMeanBaseline.class,
		// ItemScorer.class).to(ItemMeanRatingItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
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
