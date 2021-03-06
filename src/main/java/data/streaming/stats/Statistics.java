package data.streaming.stats;

import java.util.Set;

import org.grouplens.lenskit.ItemRecommender;

import com.mongodb.BasicDBObject;

import data.scraping.jsoup.JsoupResearcherDailyScraping;
import data.streaming.dto.PatentDTO;
import data.streaming.dto.ResearcherDTO;
import data.streaming.mongo.MongoDepartments;
import data.streaming.mongo.MongoGroups;
import data.streaming.mongo.MongoKeywords;
import data.streaming.mongo.MongoPatentsRating;
import data.streaming.mongo.MongoRecommendations;
import data.streaming.mongo.MongoRecommendationsPatents;
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
            	 * Tweets c�lculados (keyword, time, total count)
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection tweetsCalculated */
    			MongoTweetCalculated.getTweetsCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el n�mero de tweets por (keyword, time, total count) */
            	MongoKeywords.tweetsCalculated();
            	
            	/**
            	 * Tweets c�lculados (language, time, total count)
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection tweetsLanguageCalculated */
    			MongoTweetLanguageCalculated.getTweetsLanguageCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el n�mero de tweets por (language, time, total count) */
            	MongoKeywords.tweetsLanguageCalculated();
            	
            	/**
            	 * Departamentos c�lculados
            	 * 
            	 * */
            	
            	/* Calculamos los departamentos que tengan m�s de 100 investigadores */
            	MongoDepartments.viewDepartmentsCalculated();
            	
            	/**
            	 * Grupos c�lculados
            	 * 
            	 * */
            	
            	/* Calculamos los grupos que tengan m�s de 40 investigadores */
            	MongoGroups.viewGroupsCalculated();
            	
            	/**
            	 * Researchers c�lculados
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
            	
            	/* Sistema de recomendaci�n, calculamos el rating por cada par de investigadoes */
            	MongoResearchersRating.ratingCalculated();
            	
            	/**
            	 * Ratings Patents
            	 * 
            	 * */
    			
            	/* Elimino los elementos de la collection patentsRating */
            	MongoPatentsRating.getPatentsRatingCollection().deleteMany(document);
            	
            	/* Sistema de recomendaci�n, calculamos el rating por cada par de patentes */
            	MongoPatentsRating.ratingCalculated();
            	
            	/**
            	 * Sistema de recomendaci�n researchers
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection researchers rating */
            	MongoRecommendations.getRecommendationsCollection().deleteMany(document);
            	
            	/* Genero las recomendaciones */
            	Set<ResearcherDTO> set = Utils.researcherDTOs();
				ItemRecommender irec = Utils.getRecommender(set);
				Utils.saveModel(irec, set);
            	
				/**
            	 * Sistema de recomendaci�n patents
            	 * 
            	 * */
            	
            	/* Elimino los elementos de la collection researchers rating */
            	MongoRecommendationsPatents.getRecommendationsCollection().deleteMany(document);
            	
            	/* Genero las recomendaciones */
            	Set<PatentDTO> setPatents = Utils.patentsDTOs();
				ItemRecommender irecPatents = Utils.getRecommenderPatents(setPatents);
				Utils.saveModelPatents(irecPatents, setPatents);
				
            	
            	/**
            	 * Web scraping
            	 * 
            	 * */
            	
            	/* Web scraping diario de investigadores nuevos. Se almacenar�n en una collection auxiliar. */
            	JsoupResearcherDailyScraping.dailyScraping();
            	
            	
            } catch (Exception e) {
                    e.printStackTrace();
            }
            System.out.println("Throwing ... ");
    }

}
