package data.streaming.mongo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoTweetLanguageCalculated {
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docTweetsLanguageCalculated = db.getCollection("tweetsLanguageCalculated");
	
	/* Método usado para obtener la collection de tweets calculados */
	public static MongoCollection<org.bson.Document> getTweetsLanguageCalculatedCollection(){
	    return docTweetsLanguageCalculated;
	}
	
	/* Método usado para almacenar todos los tweets filtrados por languages */
	public static void save(List<org.bson.Document> tweetsLanguageCalculated) {
		docTweetsLanguageCalculated.insertMany(tweetsLanguageCalculated);
	}
	
	/* Método usado para generar un documento dado una keyword, date, count */
	public static org.bson.Document convertDocument(String language, String date, Integer count){
		
		//Convierto la fecha de Twitter a Date
		Date sysdate = new Date();
		
		//Formato dd/MM/yyyy
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
        String createdAt = formatter.format(sysdate);
		
		org.bson.Document document = new org.bson.Document("createdAt", createdAt)
                .append("language", language)
                .append("date", date)
                .append("count", count);
		
		return document;
	}

}
