package data.streaming.mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoResearchers {
	
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docResearchersCalculated = db.getCollection("researchersCalculated");
    
    /* Método usado para obtener la collection de researchers */
	public static MongoCollection<org.bson.Document> getResearchersCollection(){
	    return docResearchersCalculated;
	}

	public static void viewReseachersCalculated() {
		/* Elimino todos los datos de la collection */
		BasicDBObject doc = new BasicDBObject();
		getResearchersCollection().deleteMany(doc);
		
		JSONParser parser = new JSONParser();

        try {         
            URL oracle = new URL("https://si1718-dfr-researchers.herokuapp.com/api/v1/researchers"); // URL to Parse
            URLConnection yc = oracle.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream(), "UTF-8"));
            int researchersWithORCID = 0;
            int researchersWithoutORCID = 0;
            
            String inputLine;
            while ((inputLine = in.readLine()) != null) {               
                JSONArray a = (JSONArray) parser.parse(inputLine);
                
                for (Object o : a) {
                    JSONObject researchers = (JSONObject) o;

                    String orcid = (String) researchers.get("orcid");
                    
                    if (orcid != null && !orcid.equals("null") && !orcid.equals("")) {
                    	researchersWithORCID++;
                    }else {
                    	researchersWithoutORCID++;
                    }
                    
                }
            }
            in.close();
            
            /* Almacena los datos en la collection */
            
            //Convierto la fecha de Twitter a Date
    		Date sysdate = new Date();
    		
    		//Formato dd/MM/yyyy
    		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
            String createdAt = formatter.format(sysdate);
            
            org.bson.Document document = new org.bson.Document("createdAt", createdAt)
                    .append("researchersWithORCID", researchersWithORCID)
                    .append("researchersWithoutORCID", researchersWithoutORCID);
            
            getResearchersCollection().insertOne(document);
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }  

	}

}
