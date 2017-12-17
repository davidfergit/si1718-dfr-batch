package data.streaming.stats;

import com.mongodb.BasicDBObject;

import data.scraping.jsoup.JsoupResearcher;
import data.streaming.mongo.MongoKeywords;
import data.streaming.mongo.MongoResearchersRating;
import data.streaming.mongo.MongoTweetCalculated;

public class Statistics implements Runnable {
	
	@Override
    public void run() {
            System.out.println("Sleeping ...");
            try {
            	
            	/* Elimino los elementos de la BBDD */
            	BasicDBObject document = new BasicDBObject();
    			MongoTweetCalculated.getTweetsCalculatedCollection().deleteMany(document);
            	
            	/* Calculamos el número de tweets por (keyword, time, total count) */
            	MongoKeywords.tweetsCalculated();
            	
            	/* Elimino los elementos de la BBDD */
            	MongoResearchersRating.getResearchersRatingCollection().deleteMany(document);
            	
            	/* Sistema de recomendación, calculamos el rating por cada par de investigadoes */
            	MongoResearchersRating.ratingCalculated();
            	
            	/* Web scraping diario de investigadores. Se almacenará en una BBDD auxiliar */
            	JsoupResearcher.dailyScraping();
            	
            } catch (Exception e) {
                    e.printStackTrace();
            }
            System.out.println("Throwing ... ");
    }

}
