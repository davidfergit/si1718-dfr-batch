package data.streaming.mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoPatentsRating {
	
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docPatentsRating = db.getCollection("patentsRating");
    
    /* Método usado para obtener la collection de patentsRating */
	public static MongoCollection<org.bson.Document> getPatentsRatingCollection(){
	    return docPatentsRating;
	}
	
	/* Método usado para almacenar todos los ratings de las patentes */
	public static void save(List<org.bson.Document> patentsRating) {
		docPatentsRating.insertMany(patentsRating);
	}
	
	public static org.bson.Document releationship(String firstPatent, String secondPatent, Integer rating, Set<String> researchers){
		//Convierto la fecha de Twitter a Date
		Date sysdate = new Date();
		
		//Formato dd/MM/yyyy
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
        String createdAt = formatter.format(sysdate);
		
		org.bson.Document document = new org.bson.Document("firstPatent", firstPatent)
                .append("secondPatent", secondPatent)
                .append("researchers", researchers)
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
	public static void ratingCalculated(){
		
		try {         
			JSONParser parser = new JSONParser();
			URL oracle = new URL("https://si1718-rrv-patents.herokuapp.com/api/v1/patents"); // URL to Parse
	        URLConnection yc = oracle.openConnection();
	        BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream(), "UTF-8"));
	        List<org.bson.Document> patents = new ArrayList<>();
            String inputLine;
            int limit = 0;
            
            while ((inputLine = in.readLine()) != null) {               
                JSONArray a = (JSONArray) parser.parse(inputLine);
                
                for (Object o : a) {
                    JSONObject patentsJSON = (JSONObject) o;
                    
                    String idPatent = (String) patentsJSON.get("idPatent");
                    List<String> researchers = (List<String>) patentsJSON.get("inventors");
                    List<String> keywords = (List<String>) patentsJSON.get("keywords");
                    
                    //Convierto la fecha de Twitter a Date
            		Date sysdate = new Date();
            		
            		//Formato dd/MM/yyyy
            		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
                    String createdAt = formatter.format(sysdate);
                	
                	org.bson.Document document = new org.bson.Document("createdAt", createdAt)
                            .append("idPatent", idPatent)
                            .append("researchers", researchers)
                            .append("keywords", keywords);
                	
                	patents.add(document);
                    
                    limit++;
                    
                    if (limit == 2000) {
                    	break;
                    }
                }
            }
            in.close();
            
            /* Almacena los datos en la collection */
            if (patents != null && !patents.isEmpty() && patents.size() > 0) {
            	//getPatentsRatingCollection().insertMany(patents);
            }
            
            /**
    		 * SEGUNDA PARTE DEDICADA A LA RELACION ENTRE PATENTES
    		 */
    		
    		Iterable<org.bson.Document> patentsCopy1 = patents;
    		Iterable<org.bson.Document> patentsCopy2 = patents;
    		List<org.bson.Document> totalRelationship = new ArrayList<>();
    		int lote = 0;
    		
    		for (org.bson.Document firstDoc: patentsCopy1) {
    			
    			if (firstDoc.get("keywords") != null && !firstDoc.get("keywords").equals("")) {
    				
    				for (org.bson.Document secondDoc: patentsCopy2) {
    					
    					if (!firstDoc.getString("idPatent").equals(secondDoc.getString("idPatent")) && 
    							secondDoc.get("keywords") != null && !secondDoc.get("keywords").equals("")) {
    						
    						List<String> firstKeyAux = new ArrayList<>();
    						List<String> secondKeyAux = new ArrayList<>();
    						JSONArray firstJsonArray = (JSONArray) firstDoc.get("keywords");
    						JSONArray secondJsonArray = (JSONArray) secondDoc.get("keywords");

    						for (int i = 0; i < firstJsonArray.size(); i++) {
    							firstKeyAux.add((String) firstJsonArray.get(i));  
    						}
    						
    						for (int i = 0; i < secondJsonArray.size(); i++) {
    							secondKeyAux.add((String) secondJsonArray.get(i));  
    						}
    						
    						String[] firstResearchers = new String[firstKeyAux.size()];
    						firstResearchers = firstKeyAux.toArray(firstResearchers);
    						
    						String[] secondResearchers = new String[secondKeyAux.size()];
    						secondResearchers = secondKeyAux.toArray(secondResearchers);
    						
    						Integer rating = findMatchCount( firstResearchers, secondResearchers);
    						
    						if (rating > 0) {
    							Set<String> researchers = new HashSet<>();
    							
    							JSONArray researchers1 = (JSONArray) firstDoc.get("researchers");

        						for (int i = 0; i < researchers1.size(); i++) {
        							researchers.add((String) researchers1.get(i));  
        						}
        						
        						JSONArray researchers2 = (JSONArray) secondDoc.get("researchers");

        						for (int i = 0; i < researchers2.size(); i++) {
        							researchers.add((String) researchers2.get(i));  
        						}
        						
    							org.bson.Document docRelationship = releationship(firstDoc.getString("idPatent"), secondDoc.getString("idPatent"), rating, researchers);
    							System.out.println(docRelationship.toJson());
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
    			/* Aqui almaceno cada patente con su relación con el resto y su rating */
    			save(totalRelationship);
    			System.out.println("********************** FIN ***************************");
    		}
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
		
	}
	
	
}
