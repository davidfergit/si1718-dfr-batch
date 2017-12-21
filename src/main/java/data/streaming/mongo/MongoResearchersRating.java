package data.streaming.mongo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoResearchersRating {
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docResearchersRating = db.getCollection("researchersRating");
    
    /* Método usado para obtener la collection researchersRating */
	public static MongoCollection<org.bson.Document> getResearchersRatingCollection(){
	    return docResearchersRating;
	}

	/* Método usado para almacenar todos los tweets filtrados por keywords */
	public static void save(List<org.bson.Document> researchersRating) {
		docResearchersRating.insertMany(researchersRating);
	}
	
	public static org.bson.Document releationship(String firstResearcher, String secondResearcher, Integer rating){
		//Convierto la fecha de Twitter a Date
		Date sysdate = new Date();
		
		//Formato dd/MM/yyyy
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
        String createdAt = formatter.format(sysdate);
		
		org.bson.Document document = new org.bson.Document("firstReseracher", firstResearcher)
                .append("secondResearcher", secondResearcher)
                .append("rating", rating)
                .append("createdAt", createdAt);
		
		return document;
	}
	
	
	/******************************************************* MÉTODOS PARA EL BATCH ****************************************************/
	
	public static int findMatchCount(final String [] a,final String [] b){
	    int matchCount = 0;

	    for(int i = 0, j = 0;i < a.length && j < b.length;){
	        int res = a[i].compareTo(b[j]);
	        if(res == 0){
	            matchCount++;
	            i++;
	            j++;
	        }else if(res < 0){
	            i++;
	        }else{
	            j++;
	        }
	    }
	    return matchCount;
	}
	
	/* Método usado para calcular todos los tweets almacenados */
	public static void ratingCalculated() {
		
		Iterable<org.bson.Document> researchersCopy1 = MongoKeywords.getResearchersCollection().find().limit(150);
		Iterable<org.bson.Document> researchersCopy2 = researchersCopy1;
		List<org.bson.Document> totalRelationship = new ArrayList<>();
		int lote = 0;
		
		for (org.bson.Document firstDoc: researchersCopy1) {
			
			if (firstDoc.get("keywords") != null && !firstDoc.get("keywords").equals("")) {
				
				for (org.bson.Document secondDoc: researchersCopy2) {
					
					if (!firstDoc.getString("idResearcher").equals(secondDoc.getString("idResearcher")) && 
							secondDoc.get("keywords") != null && !secondDoc.get("keywords").equals("")) {
						
						Integer rating = findMatchCount(firstDoc.getString("keywords").split(","), secondDoc.getString("keywords").split(","));
						
						if (rating > 0) {
							org.bson.Document docRelationship = releationship(firstDoc.getString("idResearcher"), secondDoc.getString("idResearcher"), rating);
							totalRelationship.add(docRelationship);
							lote++;
							
							/* Añado lotes de 1000 elementos */
							if (lote == 10000 && totalRelationship != null && !totalRelationship.isEmpty() && totalRelationship.size() > 0) {
								/* Aqui almaceno cada researcher con su relación con el resto y su rating */
								save(totalRelationship);
								lote=0;
								totalRelationship = new ArrayList<>();
							}
						}//Cierra la condición rating
						
					}//Cierra la primera condición
					
				}
			}
			
		}
		
		if (totalRelationship != null && !totalRelationship.isEmpty() && totalRelationship.size() > 0) {
			/* Aqui almaceno cada researcher con su relación con el resto y su rating */
			save(totalRelationship);
			System.out.println("********************** FIN ***************************");
		}
		
	}
	
	
}
