package data.scraping.jsoup;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class JsoupResearcher {
	
	private static final String URL_BASE = "http://investigacion.us.es/sisius/sisius.php";
	private static final MongoClientURI uri  = new MongoClientURI("mongodb://researchers:researchers@ds255455.mlab.com:55455/si1718-dfr-researchers"); 
    private static final MongoClient client = new MongoClient(uri);
    private static MongoDatabase db = client.getDatabase(uri.getDatabase());
    private static final MongoCollection<org.bson.Document> docResearchers = db.getCollection("dailyResearchers");
	
	public static void dailyScraping() throws MalformedURLException, IOException {
		
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
            		String keywords = null;
            		
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
            		
            		/* Genero keyword a todos los investigadores que tengan un grupo asignado */
            		if (researcher.getGroup() != null && !researcher.getGroup().equals("")) {
            			keywords = researcher.getGroup().replace(" ", ",");
            		}
            		
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
                            .append("keywords", keywords)
                            .append("viewURL", "https://si1718-dfr-researchers.herokuapp.com/#!/researchers/" + researcher.getIdResearcher() + "/view")
                            .append("idDepartment", researcher.getDepartment())
                            .append("departmentViewURL", null)
                            .append("departmentName", null)
                    		.append("createdAt", createdAt);
    
                    /* INSERTAR EL DOCUMENTO DE TODOS LOS RESEARCHER EN UNA SOLA LLAMADA (LLAMANDO A MLAB DIRECTAMENTE) */
                    if (docResearcher != null) {
                    	researchersToInsert.add(docResearcher);
                    }
   
                    /* INCREMENTO LA VARIABLE LOTERESEARCHER */
                    loteResearchers++;
                }
            }
            
            /* USADO PARA LOTES */
            if (loteResearchers == 500) {
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

}
