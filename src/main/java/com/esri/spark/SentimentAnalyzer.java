package com.esri.spark;

import java.io.Serializable;
import java.util.Properties;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalyzer  implements Serializable{
   
	private static final long serialVersionUID = 1L;
	static StanfordCoreNLP pipeline;	
    static String local_path = "C:/Users/VENU4461/Source/Repos/kafka-spark-es/src/main/resources/";
    
    public SentimentAnalyzer() {    	
    }
    
    public void init() throws Exception{
    	//String profileDir = local_path + "profiles";
		//System.err.println(String.format("Loadind profiles from %s", profileDir));
    	Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,parse,sentiment");
		props.setProperty("parse.model", local_path + "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
        pipeline = new StanfordCoreNLP(props);
       
        String profileDir = local_path + "profiles";
		System.err.println(String.format("Loading profiles from %s", profileDir));
		DetectorFactory.loadProfile(profileDir); 		       
    }

    public int findSentiment(String tweet) {

        int mainSentiment = 0;
		try{
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
		}catch(Exception ex){
			System.err.println(ex.toString());
		}
        return mainSentiment;
    }
    
	public String detectLanguage(String text) throws Exception {
		try{
		Detector detector = DetectorFactory.create();
        detector.append(text);
        return detector.detect();
		}catch(Exception ex){
			System.err.println(ex.toString());
			return "null";
		}
	}
}
