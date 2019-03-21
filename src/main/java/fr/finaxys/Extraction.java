package fr.finaxys;


import com.textrazor.AnalysisException;
import com.textrazor.NetworkException;
import com.textrazor.TextRazor;

import com.textrazor.annotations.AnalyzedText;


import java.util.ArrayList;
import java.util.List;


import com.textrazor.annotations.Topic;
import org.json.simple.JSONObject;

import org.apache.tika.metadata.*;


public class Extraction {

    public static String extract(String text, Metadata metadata) throws NetworkException, AnalysisException {

        // this is the text razor API
        TextRazor client = new TextRazor("53a532963a463c0fe8d4dadfa05ffd9597ffa313884d82089ca8e040");

        //list of extractor used to analyse text
        client.addExtractor("words");
        client.addExtractor("entities");
        client.addExtractor(("topics"));

        AnalyzedText response = client.analyze(text);


        // JSON object containing the topics
        JSONObject topicsJSON = new JSONObject();

        // Empty JSON object containing the row that must be saved to elasticsearch
        JSONObject rowJSON = new JSONObject();



        // adding list of topics to json object
        List<String> topicsList= new ArrayList<String>();

        for (Topic topic  : response.getResponse().getTopics()) {

            topicsList.add(topic.getLabel());
        }


        rowJSON.put("topics",topicsList);


        // creation of an array containing list of metadata names
        String[] metadataList={"date","pdf:docinfo:title","pdf:PDFVersion","xmp:CreatorTool","modified","pdf:docinfo:creator","created","xmpTPg:NPages","Last-Modified"};


        // adding metadata to json object

        for (String data: metadataList
        ) {rowJSON.put(data,metadata.get(data));

        }

        // returning the json object containing list of topics and metadata of the pdf files
        // returned result will be saved to elasticsearch as a json object

        return(rowJSON.toString());




    }
}
