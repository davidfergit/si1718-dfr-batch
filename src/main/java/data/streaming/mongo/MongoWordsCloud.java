package data.streaming.mongo;

import java.text.DateFormat;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoWordsCloud {
	
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docWordsCloud = db.getCollection("wordsCloud");
    private static final MongoCollection<org.bson.Document> docResearchers = db.getCollection("researchers");
    
    /* Método usado para obtener la collection de wordsCloud */
	public static MongoCollection<org.bson.Document> getWordsCloudCollection(){
	    return docWordsCloud;
	}
	
	/* Método usado para obtener la collection de keywords */
	public static MongoCollection<org.bson.Document> getResearchersCollection(){
	    return docResearchers;
	}


	public static void main(String[] args) {
		FindIterable<org.bson.Document> researchers = getResearchersCollection().find();
		List<String> totalKeywords = new ArrayList<String>();
		List<org.bson.Document> wordsCloud = new ArrayList<org.bson.Document>();
		
		for (org.bson.Document researcher: researchers) {
			/* Obtengo el campo keywords */
			if (researcher.getString("idGroup") != null && !researcher.getString("idGroup").equals("") &&
				researcher.getString("idDepartment") != null && !researcher.getString("idDepartment").equals("")) {
				List<String> keywords = Arrays.asList(researcher.getString("keywords").split(","));
				for (int i = 0; i < keywords.size(); i++) {
					keywords.set(i, Normalizer.normalize(keywords.get(i), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
				}
				totalKeywords.addAll(keywords);
			}
		}
		
		/* Filtro para evitar que aparezcan keywords vacías/nulas */
		Set<String> unique = new HashSet<String>(totalKeywords);
		unique = unique.stream()
				.filter(s -> s.length() % 2 != 0)
				.collect(Collectors.toSet());
		
		for (String key : unique) {
		    if (!key.equals(":") && !key.equals("&") && !key.equals("(")) {
	    		Date sysdate = new Date();
	    		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
	            String createdAt = formatter.format(sysdate);
	            
	            org.bson.Document document = new org.bson.Document("createdAt", createdAt)
	                    .append("text", key)
	                    .append("weight", Collections.frequency(totalKeywords, key));
	            
	            wordsCloud.add(document);
		    }
		}
		
		if (wordsCloud != null && wordsCloud.size() > 0) {
			BasicDBObject document = new BasicDBObject();
			getWordsCloudCollection().deleteMany(document);
			getWordsCloudCollection().insertMany(wordsCloud);
		}

	}

}
