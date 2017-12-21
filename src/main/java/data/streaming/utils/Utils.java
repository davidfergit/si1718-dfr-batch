package data.streaming.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.shaded.com.google.common.collect.Maps;
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
import org.grouplens.lenskit.scored.ScoredId;

import com.fasterxml.jackson.databind.ObjectMapper;

import data.streaming.dto.ResearcherDTO;
import data.streaming.dto.TweetDTO;
import data.streaming.mongo.MongoRecommendations;
import data.streaming.mongo.MongoResearchersRating;

public class Utils {
	
	
	public static final String[] TAGNAMES = { "#TheWalkingDeadUK" };
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final int MAX_RECOMMENDATIONS = 3;
	
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
	
	public static ItemRecommender getRecommender(Set<ResearcherDTO> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);
		// config.bind(BaselineScorer.class,
		// ItemScorer.class).to(UserMeanItemScorer.class);
		// config.bind(UserMeanBaseline.class,
		// ItemScorer.class).to(ItemMeanRatingItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
	}

	private static Collection<? extends Event> createEventCollection(Set<ResearcherDTO> ratings) {
		List<Event> result = new LinkedList<>();

		for (ResearcherDTO dto : ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getFirstResearcher().hashCode());
			r.setUserId(dto.getSecondResearcher().hashCode());
			r.setRating(dto.getRating());
			result.add(r);
		}
		return result;
	}
	
	public static void saveModel(ItemRecommender irec, Set<ResearcherDTO> set) throws IOException {
		Map<String, Long> keys = Maps.asMap(set.stream().map((ResearcherDTO x) -> x.getFirstResearcher()).collect(Collectors.toSet()),
				(String y) -> new Long(y.hashCode()));
		Map<Long, List<String>> reverse = set.stream().map((ResearcherDTO x) -> x.getFirstResearcher())
				.collect(Collectors.groupingBy((String x) -> new Long(x.hashCode())));
		List<org.bson.Document> result = new ArrayList<>();

		for (String key : keys.keySet()) {
			List<ScoredId> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS);
			if (recommendations.size() > 0) {
				/* System.out.println(key + "->" + recommendations.stream().map(x -> reverse.get(x.getId()).get(0))
						.collect(Collectors.toList()));*/
				result.add(MongoRecommendations.recommendation(key, recommendations.stream().map(x -> reverse.get(x.getId()).get(0))
						.collect(Collectors.toList())));
			}
		}
		
		/* Almaceno las recomendaciones en mLab*/
		MongoRecommendations.save(result);
	}
	
	public static Set<ResearcherDTO> researcherDTOs() {
		/* Obtengo las keywords con su correspondiente rating */
		Iterable<org.bson.Document> ratings = MongoResearchersRating.getResearchersRatingCollection().find();
		Set<ResearcherDTO> result = new HashSet<>();
	
		for (org.bson.Document dto: ratings) {
			ResearcherDTO researcherDTO = new ResearcherDTO();
			researcherDTO.setFirstResearcher(dto.getString("firstResearcher"));
			researcherDTO.setSecondResearcher(dto.getString("secondResearcher"));
			researcherDTO.setRating(dto.getInteger("rating"));
			result.add(researcherDTO);
		}
		return result;
	}

}
