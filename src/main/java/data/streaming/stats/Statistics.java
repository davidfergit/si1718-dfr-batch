package data.streaming.stats;

import java.util.Set;

import org.grouplens.lenskit.ItemRecommender;

import com.mongodb.BasicDBObject;

import data.scraping.jsoup.JsoupResearcher;
import data.streaming.dto.ResearcherDTO;
import data.streaming.mongo.MongoDepartments;
import data.streaming.mongo.MongoGroups;
import data.streaming.mongo.MongoKeywords;
import data.streaming.mongo.MongoPatentsRating;
import data.streaming.mongo.MongoRecommendations;
import data.streaming.mongo.MongoResearchers;
import data.streaming.mongo.MongoResearchersRating;
import data.streaming.mongo.MongoTweetCalculated;
import data.streaming.mongo.MongoTweetLanguageCalculated;
import data.streaming.utils.Utils;

public class Statistics implements Runnable {
	
	@Override
    public void run() {
            System.out.println("Sleeping ...");
            try {
            	
            	BasicDBObject document = new BasicDBObject();
            	
            	/**
            	 * Tweets cálculados (keyword, time, total count)
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection tweetsCalculated */
    			MongoTweetCalculated.getTweetsCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el número de tweets por (keyword, time, total count) */
            	MongoKeywords.tweetsCalculated();
            	
            	/**
            	 * Tweets cálculados (language, time, total count)
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection tweetsLanguageCalculated */
    			MongoTweetLanguageCalculated.getTweetsLanguageCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el número de tweets por (language, time, total count) */
            	MongoKeywords.tweetsLanguageCalculated();
            	
            	/**
            	 * Departamentos cálculados
            	 * 
            	 * */
            	
            	/* Calculamos los departamentos que tengan más de 100 investigadores */
            	MongoDepartments.viewDepartmentsCalculated();
            	
            	/**
            	 * Grupos cálculados
            	 * 
            	 * */
            	
            	/* Calculamos los grupos que tengan más de 40 investigadores */
            	MongoGroups.viewGroupsCalculated();
            	
            	/**
            	 * Researchers cálculados
            	 * 
            	 * */
            	
            	/* Calculamos los investigadores que tienen/no tienen ORCID */
            	MongoResearchers.viewReseachersCalculated();
            	
    			
    			/**
            	 * Ratings Researchers
            	 * 
            	 * */
    			
            	/* Elimino los elementos de la collection researchersRating */
            	MongoResearchersRating.getResearchersRatingCollection().deleteMany(document);
            	
            	/* Sistema de recomendación, calculamos el rating por cada par de investigadoes */
            	MongoResearchersRating.ratingCalculated();
            	
            	/**
            	 * Ratings Patents
            	 * 
            	 * */
    			
            	/* Elimino los elementos de la collection patentsRating */
            	MongoPatentsRating.getPatentsRatingCollection().deleteMany(document);
            	
            	/* Sistema de recomendación, calculamos el rating por cada par de patentes */
            	MongoPatentsRating.ratingCalculated();
            	
            	/**
            	 * Sistema de recomendación researchers
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection researchers rating */
            	MongoRecommendations.getRecommendationsCollection().deleteMany(document);
            	
            	/* Genero las recomendaciones */
            	Set<ResearcherDTO> set = Utils.researcherDTOs();
				ItemRecommender irec = Utils.getRecommender(set);
				Utils.saveModel(irec, set);
            	
				/**
            	 * Sistema de recomendación patents
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection researchers rating */
            	//MongoRecommendations.getRecommendationsCollection().deleteMany(document);
            	
            	/* Genero las recomendaciones */
            	//Set<ResearcherDTO> set = Utils.researcherDTOs();
				//ItemRecommender irec = Utils.getRecommender(set);
				//Utils.saveModel(irec, set);
				
            	
            	/**
            	 * Web scraping
            	 * 
            	 * */
            	
            	/* Web scraping diario de investigadores nuevos. Se almacenarán en una collection auxiliar. */
            	JsoupResearcher.dailyScraping();
            	
            	
            } catch (Exception e) {
                    e.printStackTrace();
            }
            System.out.println("Throwing ... ");
    }

}
