package data.streaming.mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoGroups {
	
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docGroups = db.getCollection("groupsCalculated");
    
    /* Método usado para obtener la collection de groups */
	public static MongoCollection<org.bson.Document> getGroupsCollection(){
	    return docGroups;
	}

	public static void viewGroupsCalculated() {
		/* Elimino todos los datos de la collection */
		BasicDBObject doc = new BasicDBObject();
		getGroupsCollection().deleteMany(doc);
		
		JSONParser parser = new JSONParser();

        try {         
            URL oracle = new URL("https://si1718-rgg-groups.herokuapp.com/api/v1/groups"); // URL to Parse
            URLConnection yc = oracle.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream(), "UTF-8"));
            List<org.bson.Document> groups = new ArrayList<>();
            
            String inputLine;
            while ((inputLine = in.readLine()) != null) {               
                JSONArray a = (JSONArray) parser.parse(inputLine);
                
                for (Object o : a) {
                    JSONObject groupJSON = (JSONObject) o;

                    List<String> researchers = (List<String>) groupJSON.get("components");
                    
                    if (researchers.size() > 40) {
                    	String group = (String) groupJSON.get("name");
                    	String idGroup = (String) groupJSON.get("idGroup");
                    	String url = "https://si1718-rgg-groups.herokuapp.com/#!/group/" + idGroup;
                    	
                    	//Convierto la fecha de Twitter a Date
                		Date sysdate = new Date();
                		
                		//Formato dd/MM/yyyy
                		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
                        String createdAt = formatter.format(sysdate);
                    	
                    	org.bson.Document document = new org.bson.Document("createdAt", createdAt)
                                .append("count", researchers.size())
                                .append("_id", group)
                                .append("url", url);
                    	
                    	groups.add(document);
                    }
                    
                }
            }
            in.close();
            
            /* Almacena los datos en la collection */
            if (groups != null && !groups.isEmpty() && groups.size() > 0) {
            	getGroupsCollection().insertMany(groups);
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
