package data.scraping.jsoup;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class JsoupResearcherDailyScraping {
	
	private static final String URL_BASE = "http://investigacion.us.es/sisius/sisius.php";
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docResearchers = db.getCollection("dailyResearchers");
    private static final MongoCollection<org.bson.Document> docResearchersAux = db.getCollection("researchers");
    
	public static void dailyScraping() throws MalformedURLException, IOException {
		
    	/* Elimino los elementos de la collection dailyResearchers */
		BasicDBObject document = new BasicDBObject();
		docResearchers.deleteMany(document);
		
		/* Genero researchers nuevos para testear la funcionalidad de insertar aquellos investigadores que hayan aparecido recientemente. */
		createRandomResearchers(3);
		
		/* Obtengo todos los investigadores */
		Document researchers = Jsoup
                .connect(URL_BASE)
                .data("text2search", "%%%")
				.data("en", "1")
                .data("inside", "1")
                .maxBodySize(10 * 1024 * 1024)
				.post();
        
        /* INSERTAR EL DOCUMENTO DE TODOS LOS RESEARCHER EN UNA SOLA LLAMADA (LLAMANDO A MLAB DIRECTAMENTE) */
        List<org.bson.Document> researchersToInsert = new ArrayList<org.bson.Document>();
        int loteResearchers = 0;

		Elements elements = researchers.select("td.data a");
        int i = 0;

        for(Iterator<Element> iterator = elements.iterator(); iterator.hasNext(); ) {

            Element element = iterator.next();

            if(i % 2 != 1) {

                String link = "https://investigacion.us.es" + element.attr("href");

                if(link.contains("sis_showpub.php")) {

                	Document doc = Jsoup.parse(new URL(link), 10000);
            		Elements elementos = doc.getElementsByAttribute("align");
            		Element top = elementos.get(0).getElementsByTag("p").get(0);
            		List<String> tagsSplit = Arrays.asList(top.toString().replace("<p>", "").replace("</p>", "").split("<br>"));
            		Researcher researcher = new Researcher();
            		
            		/* Nombre */
            		if (tagsSplit.size() >= 0 && tagsSplit.get(0) != null && !tagsSplit.get(0).equals("")) {
            			researcher.setName(tagsSplit.get(0));
            		}	
            		
            		/* Teléfono */
            		String phone = top.toString();
            		if (phone.contains("Telefono:")) {
            			String phoneAux = phone.substring(phone.indexOf("Telefono:"), phone.length());
            			phoneAux = phoneAux.substring(0, phoneAux.indexOf("<br>"));
            			phoneAux = phoneAux.replace("Telefono: ", "");
            			researcher.setPhone(phoneAux);
            		}else if (phone.contains("Teléfono:")){
            			String phoneAux = phone.substring(phone.indexOf("Teléfono:"), phone.length());
            			phoneAux = phoneAux.substring(0, phoneAux.indexOf("<br>"));
            			phoneAux = phoneAux.replace("Teléfono: ", "");
            			researcher.setPhone(phoneAux);
            		}
            		
            		/* Situación Profesional */
            		String professionalSituation = top.toString();
            		if (professionalSituation.contains("Situación profesional:")) {
            			String professionalSituationAux = professionalSituation.substring(professionalSituation.indexOf("Situación profesional:"), professionalSituation.length());
            			professionalSituationAux = professionalSituationAux.substring(0, professionalSituationAux.indexOf("<br>"));
            			professionalSituationAux = professionalSituationAux.replace("Situación profesional: ", "");
            			researcher.setProfessionalSituation(professionalSituationAux);
            		}else if (professionalSituation.contains("Situacion profesional:")){
            			String professionalSituationAux = professionalSituation.substring(professionalSituation.indexOf("Situacion profesional:"), professionalSituation.length());
            			professionalSituationAux = professionalSituationAux.substring(0, professionalSituationAux.indexOf("<br>"));
            			professionalSituationAux = professionalSituationAux.replace("Situacion profesional: ", "");
            			researcher.setProfessionalSituation(professionalSituationAux);
            		}
            		
            		/* Separo las etiquetas <a> */	
            		Elements tagsA = top.getElementsByTag("a");
            		for (Iterator<Element> it = tagsA.iterator(); it.hasNext();) {
            			Element e = it.next();
            			
            			if (e.attr("href").contains("orcid")) {				
            				/* ORCID */
            				if (!e.text().equals("")) {
            					researcher.setOrcid(e.text());
            				}	
            			}
            			
            			/* RsearcherId */
            			if (e.attr("href").contains("researcherid")) {
            				if (!e.text().equals("")) {
            					researcher.setResearcherId(e.text());
            				}	
            			}
            			
            			/* AnotherLink (Scholar) */
            			if ((!e.attr("href").contains("orcid") && !e.attr("href").contains("researcherid") &&
            				!e.attr("href").contains("sis_depgrupos.php") && !e.attr("href").contains("sis_dep.php") 
            				&& !e.attr("href").contains("sis_solmail.php")) || e.attr("href").contains("scholar")) {
            				if (!e.text().equals("")) {
            					researcher.setLink(e.text());
            				}
            			}
            			
            			/* Grupo */
            			if (e.attr("href").contains("sis_depgrupos.php")) {
            				if (!e.text().equals("")) {
            					researcher.setGroup(e.text());
            				}
            			}
            			
            			/* Departamento/Unidad */
            			if (e.attr("href").contains("sis_dep.php")) {
            				if (!e.text().equals("")) {
            					researcher.setDepartment(e.text());
            				}
            			}
            			
            		}
            		
            		/* Genero el identificador propio idResearcher. Este identificador sera el orcid y en caso de no tener orcid 
            		 * se recogerá el identificador obtenido por parametro (idpers)*/
            		if (researcher.getOrcid() != null && !researcher.getOrcid().equals("")) {
            			researcher.setIdResearcher(researcher.getOrcid());
            		}else {
            			researcher.setIdResearcher(link.split("=")[1]);
            		}
            		
            		/* ASIGNACION DE KEYWORDS */
            		List<String> forbiddenWords = Arrays.asList("a","ante","bajo","cabe","con","contra","de","desde","en","entre","hacia","hasta","para","por",
            				"según","sin","so","sobre","tras","durante","mediante","excepto","salvo","incluso","más","menos",
            				"el","la","los","las","un","uno","una","unos","unas", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", 
            				"ñ", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "del", "and", "&", ":", "*", "(", ")", ",", ".");
            		Set<String> keywordsAux = new HashSet<String>();
            		
            		if (researcher.getGroup() != null && !researcher.getGroup().equals("")) {
            			keywordsAux.addAll(Arrays.asList(researcher.getGroup().toLowerCase().split(" ")));
            		}else if (researcher.getDepartment() != null && !researcher.getDepartment().equals("")) {
            			keywordsAux.addAll(Arrays.asList(researcher.getDepartment().toLowerCase().split(" ")));
            		}else if (researcher.getProfessionalSituation() != null && !researcher.getProfessionalSituation().equals("")){
            			keywordsAux.addAll(Arrays.asList(researcher.getProfessionalSituation().toLowerCase().split(" ")));
            		}else if (researcher.getName() != null && !researcher.getName().equals("")){
            			keywordsAux.addAll(Arrays.asList(researcher.getName().toLowerCase().split(" ")));
            			keywordsAux.add("key-us");
            			keywordsAux.add("universidad");
            			keywordsAux.add("sevilla");
            		}else {
            			keywordsAux.add("key-us");
            			keywordsAux.add("universidad");
            			keywordsAux.add("sevilla");
            		}
            		
            		/* Elimino las palabras prohibidas */
            		keywordsAux.removeAll(forbiddenWords);
            		
            		/* Elimino los espacios en blanco */
            		keywordsAux.removeAll(keywordsAux.stream()
            			      .filter(s -> s.equals(""))
            			      .collect(Collectors.toSet()));
            		
            		/* Elimino los nulos */
            		keywordsAux.removeIf(Objects::isNull);
            		
            		String keywordsCommaSeparated = keywordsAux.stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.joining(","));
            		
            		//Convierto la fecha de Twitter a Date
            		Date sysdate = new Date();
            		
            		//Formato dd/MM/yyyy
            		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
                    String createdAt = formatter.format(sysdate);
            		
                    org.bson.Document docResearcher = new org.bson.Document("idResearcher", researcher.getIdResearcher())
                            .append("name", researcher.getName())
                            .append("phone", researcher.getPhone())
                            .append("professionalSituation", researcher.getProfessionalSituation())
                            .append("orcid", researcher.getOrcid())
                            .append("researcherId", researcher.getResearcherId())
                            .append("link", researcher.getLink())
                            .append("idGroup", researcher.getGroup())
                            .append("keywords", keywordsCommaSeparated)
                            .append("viewURL", "https://si1718-dfr-researchers.herokuapp.com/#!/researchers/" + researcher.getIdResearcher() + "/view")
                            .append("idDepartment", researcher.getDepartment())
                            .append("departmentViewURL", null)
                            .append("departmentName", null)
                    		.append("createdAt", createdAt);
                    
                    /* INSERTAR EL DOCUMENTO DE TODOS LOS RESEARCHER EN UNA SOLA LLAMADA (LLAMANDO A MLAB DIRECTAMENTE) */
                    /* Compruebo si el investigador que queremos insertar existe ya en la base de datos y de ser así se descarta. */
                    long checkNewResearcher = docResearchersAux.count(Filters.eq("idResearcher",researcher.getIdResearcher()));
                    
                    if (docResearcher != null && checkNewResearcher == 0 ) {
                    	researchersToInsert.add(docResearcher);
                    	System.out.println("NUEVO");
                    	/* INCREMENTO LA VARIABLE LOTERESEARCHER */
                        loteResearchers++;
                    }else {
                    	System.out.println("YA EXISTE");
                    }
                }
            }
            
            /* USADO PARA LOTES */
            if (loteResearchers == 500 && researchersToInsert != null && researchersToInsert.size() > 0) {
            	/* INSERTAR EL DOCUMENTO DE TODOS LOS RESEARCHER EN UNA SOLA LLAMADA (LLAMANDO A MLAB DIRECTAMENTE) */
                docResearchers.insertMany(researchersToInsert);
                researchersToInsert = new ArrayList<>();
                loteResearchers = 0;
            }
            
            i++;
        }//Cierra el iterador
        
        
        /* Compruebo que en la lista hay investigadores por insertar, ya que no se insertarón en los bloques anteriores.
         * Por lo tanto los persisto ahora en Mlab */
        if (researchersToInsert != null && researchersToInsert.size() > 0) {
        	docResearchers.insertMany(researchersToInsert);
        }
        
        /* Cierro la conexion de mongoDB */
        client.close();
		
	}
	
	public static void createRandomResearchers(int numberOfResearchers) {
		List<org.bson.Document> researchersToInsert = new ArrayList<org.bson.Document>();
		
		for (int i=1; i <= numberOfResearchers; i++) {
			//Convierto la fecha de Twitter a Date
    		Date sysdate = new Date();
    		
    		//Formato dd/MM/yyyy
    		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
            String createdAt = formatter.format(sysdate);
    		
            org.bson.Document docResearcher = new org.bson.Document("idResearcher", "investigador"+i)
                    .append("name", "Investigador Name " + i)
                    .append("phone", "Investigador Phone " + i)
                    .append("professionalSituation", "Active")
                    .append("orcid", null)
                    .append("researcherId", null)
                    .append("link", null)
                    .append("idGroup", null)
                    .append("keywords", "investigador" + i)
                    .append("viewURL", "https://si1718-dfr-researchers.herokuapp.com/#!/researchers/" + "investigador" + i + "/view")
                    .append("idDepartment", null)
                    .append("departmentViewURL", null)
                    .append("departmentName", null)
            		.append("createdAt", createdAt);
            
            researchersToInsert.add(docResearcher);
		}
		
		docResearchers.insertMany(researchersToInsert);
		
	}

}
