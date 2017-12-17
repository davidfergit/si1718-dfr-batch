package data.streaming.mongo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoTweetCalculated {
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docTweetsCalculated = db.getCollection("tweetsCalculated");
	
	/* Método usado para obtener la collection de tweets calculados */
	public static MongoCollection<org.bson.Document> getTweetsCalculatedCollection(){
	    return docTweetsCalculated;
	}
	
	/* Método usado para almacenar todos los tweets filtrados por keywords */
	public static void save(List<org.bson.Document> tweetsCalculated) {
		docTweetsCalculated.insertMany(tweetsCalculated);
	}
	
	/* Método usado para generar un documento dado una keyword, date, count */
	public static org.bson.Document convertDocument(String keyword, String date, Integer count){
		
		//Convierto la fecha de Twitter a Date
		Date sysdate = new Date();
		
		//Formato dd/MM/yyyy
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
        String createdAt = formatter.format(sysdate);
		
		org.bson.Document document = new org.bson.Document("createdAt", createdAt)
                .append("keyword", keyword)
                .append("date", date)
                .append("count", count);
		
		return document;
	}

}
