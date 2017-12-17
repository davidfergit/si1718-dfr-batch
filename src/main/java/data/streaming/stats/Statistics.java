package data.streaming.stats;

import com.mongodb.BasicDBObject;

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
            	
            	/* Calculamos el n�mero de tweets por (keyword, time, total count) */
            	MongoKeywords.tweetsCalculated();
            	
            	/* Sistema de recomendaci�n, calculamos el rating por cada par de investigadoes */
            	MongoResearchersRating.ratingCalculated();
            	
            } catch (Exception e) {
                    e.printStackTrace();
            }
            System.out.println("Throwing ... ");
    }

}
