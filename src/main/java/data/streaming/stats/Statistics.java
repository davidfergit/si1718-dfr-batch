package data.streaming.stats;

import java.util.Set;

import org.grouplens.lenskit.ItemRecommender;

import com.mongodb.BasicDBObject;

import data.scraping.jsoup.JsoupResearcher;
import data.streaming.dto.KeywordDTO;
import data.streaming.dto.ResearcherDTO;
import data.streaming.mongo.MongoKeywords;
import data.streaming.mongo.MongoResearchersRating;
import data.streaming.mongo.MongoTweetCalculated;
import data.streaming.utils.Utils;

public class Statistics implements Runnable {
	
	@Override
    public void run() {
            System.out.println("Sleeping ...");
            try {
            	
            	BasicDBObject document = new BasicDBObject();
            	
            	/**
            	 * Tweets cálculados
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection tweetsCalculated */
    			MongoTweetCalculated.getTweetsCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el número de tweets por (keyword, time, total count) */
            	MongoKeywords.tweetsCalculated();
            	
    			
    			/**
            	 * Ratings
            	 * 
            	 * */
    			
            	/* Elimino los elementos de la collection researchers rating */
            	MongoResearchersRating.getResearchersRatingCollection().deleteMany(document);
            	
            	/* Sistema de recomendación, calculamos el rating por cada par de investigadoes */
            	MongoResearchersRating.ratingCalculated();
            	
            	
            	/**
            	 * Web scraping
            	 * 
            	 * */
            	
            	/* Web scraping diario de investigadores. Se almacenará en una BBDD auxiliar */
            	JsoupResearcher.dailyScraping();
            	
            	/**
            	 * Sistema de recomendación
            	 * 
            	 * */
            	//Set<ResearcherDTO> set = Utils.researcherDTOs();
				//ItemRecommender irec = Utils.getRecommender(set);
				//Utils.saveModel(irec, set);
            	
            	
            	
            } catch (Exception e) {
                    e.printStackTrace();
            }
            System.out.println("Throwing ... ");
    }

}
