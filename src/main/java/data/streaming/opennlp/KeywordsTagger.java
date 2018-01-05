package data.streaming.opennlp;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import opennlp.tools.postag.POSModel; 
import opennlp.tools.postag.POSSample; 
import opennlp.tools.postag.POSTaggerME; 
import opennlp.tools.tokenize.WhitespaceTokenizer;

public class KeywordsTagger {

	public static void main(String[] args) throws Exception {
			
		/**
		 *  UNIVERSAL SPEECH TAGS
		 *  
		 * 	VERB - verbs (all tenses and modes)
			NOUN - nouns (common and proper)
			PRON - pronouns 
			ADJ - adjectives
			ADV - adverbs
			ADP - adpositions (prepositions and postpositions)
			CONJ - conjunctions
			DET - determiners
			NUM - cardinal numbers
			PRT - particles or other function words
			X - other: foreign words, typos, abbreviations
			. - punctuation
		 * 
		 */
	    
	    //Loading Parts of speech-maxent model       
	    InputStream inputStream = new FileInputStream("opennlp-models/opennlp-es-pos-maxent-pos-universal.model"); 
	    POSModel model = new POSModel(inputStream); 
	       
	    //Creating an object of WhitespaceTokenizer class  
	    WhitespaceTokenizer whitespaceTokenizer= WhitespaceTokenizer.INSTANCE; 
	       
	    //Tokenizing the sentence 
	    //String sentence = "Hi welcome to Tutorialspoint"; 
	    String sentence = "Union Europea y Estado Autonomico";
	    String[] tokens = whitespaceTokenizer.tokenize(sentence); 
	       
	    //Instantiating POSTaggerME class 
	    POSTaggerME tagger = new POSTaggerME(model); 
	             
	    //Generating tags 
	    String[] tags = tagger.tag(tokens);       
	      
	    //Instantiating the POSSample class 
	    POSSample sample = new POSSample(tokens, tags);  
	    System.out.println(sample.toString());
	      
	    //Probabilities for each tag of the last tagged sentence. 
	    double [] probs = tagger.probs();       
	    //System.out.println("  ");       
	      
	    //Printing the probabilities  
	    /*for(int i = 0; i<probs.length; i++) 
	       System.out.println(probs[i]);*/ 
	    
	    //System.out.println("tokens length: " + tokens.length + " tags length: " + tags.length + " probs length: " + probs.length);
	    
	    List<String> keywordsSelected = new ArrayList<String>();
	    
	    for(int i = 0; i < tokens.length; i++) {
	    	if (tags[i].equals("NOUN") && probs[i] > 0.60) {
	    		/*System.out.println(tokens[i]);
		    	System.out.println(tags[i]);
		    	System.out.println(probs[i]);*/
		    	keywordsSelected.add(tokens[i]);
	    	}
	    }
	    
	    String keywordsCommaSeparated = keywordsSelected.stream()
	                                        .collect(Collectors.joining(","));
	    
	    System.out.println(keywordsCommaSeparated);
	    
	}

}
