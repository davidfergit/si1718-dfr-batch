package data.streaming.mongo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoRecommendationsPatents {
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docRecommendations = db.getCollection("patentsRecommendations");
    
    /* Método usado para obtener la collection researchersRating */
	public static MongoCollection<org.bson.Document> getRecommendationsCollection(){
	    return docRecommendations;
	}

	/* Método usado para almacenar todos los tweets filtrados por keywords */
	public static void save(List<org.bson.Document> recommendations) {
		docRecommendations.insertMany(recommendations);
	}
	
	public static org.bson.Document recommendation(String idPatent, List<String> patents){
		//Convierto la fecha de Twitter a Date
		Date sysdate = new Date();
		
		//Formato dd/MM/yyyy
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
        String createdAt = formatter.format(sysdate);
		
		org.bson.Document document = new org.bson.Document("idPatent", idPatent)
				.append("patents", patents)
                .append("createdAt", createdAt);
	
		
		return document;
	}	
	
}
