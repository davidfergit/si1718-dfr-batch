package data.streaming.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

import data.streaming.dto.TweetDTO;

public class MongoKeywords {
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docResearchers = db.getCollection("researchers");
    private static final MongoCollection<org.bson.Document> docTweets = db.getCollection("tweets");
    
    /* Método usado para obtener la collection de researchers */
	public static MongoCollection<org.bson.Document> getResearchersCollection(){
	    return docResearchers;
	}
	
	/* Método usado para obtener la collection de tweets */
	public static MongoCollection<org.bson.Document> getTweetsCollection(){
	    return docTweets;
	}
	
	/* Método usado para almacenar un tweet */
	public static void saveTweetByKeywords(org.bson.Document tweetsFiltered) {
		docTweets.insertOne(tweetsFiltered);
	}
	
	/* Método usado para almacenar todos los tweets filtrados por keywords */
	public static void saveTweetsFilteredByKeywords(List<org.bson.Document> tweetsFiltered) {
		docTweets.insertMany(tweetsFiltered);
	}
	
	/* Método usado para generar generar un documento BSON (MongoCollection) dado un objecto TweetDTO */
	public static org.bson.Document convertTweetDTOToDocument(TweetDTO tweet){
		org.bson.Document document = new org.bson.Document("createdAt", tweet.getCreatedAt())
                .append("text", tweet.getText())
                .append("language", tweet.getLanguage());
		
		return document;
	}
	
	/* Método usado para generar generar un documento BSON (MongoCollection) dado un objecto TweetDTO */
	public static List<org.bson.Document> convertTweetDTOToDocument(List<TweetDTO> tweets){
		List<org.bson.Document> documents = new ArrayList<>();
		for (TweetDTO tweet: tweets) {
			org.bson.Document document = new org.bson.Document("createdAt", tweet.getCreatedAt())
	                .append("text", tweet.getText())
	                .append("language", tweet.getLanguage());
			documents.add(document);
		}
		return documents;
	}
	
	/* Método usado para generar generar un documento BSON (MongoCollection) dado un objecto TweetDTO y persiste en BBDD */
	public static void convertTweetDTOToDocumentAndSave(TweetDTO tweet){
		org.bson.Document document = new org.bson.Document("createdAt", tweet.getCreatedAt())
                .append("text", tweet.getText())
                .append("language", tweet.getLanguage())
                .append("idStr", tweet.getUser().getIdStr())
                .append("name", tweet.getUser().getName())
                .append("screenName", tweet.getUser().getScreenName())
                .append("friends", tweet.getUser().getFriends())
                .append("followers", tweet.getUser().getFollowers());
		
		saveTweetByKeywords(document);
	}
	
	/* Método usado para obtener todos los keywords de todos los researchers. (Keywords sin repetir) */
	public static List<String> getKeywordsOfResearchers(){
		List<String> result = null;
		result = docResearchers.distinct("keywords", String.class)
        		.into(new ArrayList<String>());
	    return result;
	}
	
//	public static String[] getKeywords(){
//		List<String> aux = null;
//		
//		/* Obtener las keywords de mongoDB */
//        aux = MongoKeywords.getKeywordsOfResearchers();
//        
//        for (int i = 0; i < aux.size(); i++) {
//        	if (aux.get(i) == null || aux.get(i).equals("null") || aux.get(i).trim().equals("null"))
//        		aux.remove(i);
//        }
//        
//        String[] result = new String[aux.size()];
//        result = aux.toArray(result);
//        
//		return result;
//	}
	
	public static String[] getKeywords(){
		String keywords[] = {"Fisica","Sintesis","Ecologia","Ixbilia","Fisicoquimica","Neuroembriologia","Ingeniería","Mineralogia"};
        
		return keywords;
	}
	
	/* Método usado para obtener todos los languages de los tweets */
	public static List<String> distinctLanguages() {
		MongoCursor<String> cursor = docTweets.distinct("language", String.class).iterator();
		List<String> languages = new ArrayList<String>();
	
		while (cursor.hasNext()) {
			languages.add(cursor.next());
		}
		
		return languages;
	}
	
	/******************************************************* MÉTODOS PARA EL BATCH ****************************************************/
	
	/* Método usado para calcular todos los tweets almacenados */
	public static List<org.bson.Document> tweetsCalculated() {
		List<org.bson.Document> statistics = new ArrayList<>();
		String keywords[] = getKeywords();
		
		if (keywords != null && keywords.length > 0) {
			
			for (String keyword: keywords) {
			    
			    for (org.bson.Document doc: getTweetsCollection().aggregate(
			    		
			    		Arrays.asList(
			    	              Aggregates.match(Filters.regex("text", ".*" + keyword + ".*")),
			    	              Aggregates.group("$createdAt", Accumulators.sum("count", 1))
			    	      )
			    		
			    		)) {
			    	
			    	/* Genero documento*/
			    	System.out.println(keyword + " --> " + doc.toJson());
			    	String date = doc.getString("_id");
			    	int count = doc.getInteger("count");
			    	statistics.add(MongoTweetCalculated.convertDocument(keyword, date, count));
			    	
			    }//Cierra el bucle que recorre todos los tweets
			    
			}//Cierra el bucle keywords
			
			/********************** Almacena los cálculos en BBDD ***********************/
			
			MongoTweetCalculated.save(statistics);
			
			/********************** Cierra conexión ***********************/
			
			
		}//Cierra el if
		
		
		
		return statistics;
	}
	
	/* Método usado para contar el número de veces que aparece un idioma en concreto (pt, es, en, etc) */
	public static List<org.bson.Document> tweetsLanguageCalculated() {
		List<org.bson.Document> statistics = new ArrayList<>();
		List<String> languages = distinctLanguages();
		
		if (languages != null && languages.size() > 0) {
			
			for (String language: languages) {
			    
			    for (org.bson.Document doc: getTweetsCollection().aggregate(
			    		
			    		Arrays.asList(
			    	              Aggregates.match(Filters.regex("language", ".*" + language + ".*")),
			    	              Aggregates.group("$createdAt", Accumulators.sum("count", 1))
			    	      )
			    		
			    		)) {
			    	
			    	/* Genero documento*/
			    	System.out.println(language + " --> " + doc.toJson());
			    	String date = doc.getString("_id");
			    	int count = doc.getInteger("count");
			    	statistics.add(MongoTweetLanguageCalculated.convertDocument(language, date, count));
			    	
			    }//Cierra el bucle que recorre todos los tweets
			    
			}//Cierra el bucle keywords
			
			/********************** Almacena los cálculos en BBDD ***********************/
			
			MongoTweetLanguageCalculated.save(statistics);
			
			/********************** Cierra conexión ***********************/
			
			
		}//Cierra el if
		
		
		
		return statistics;
	}
}
