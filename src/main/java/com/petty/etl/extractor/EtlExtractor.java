package com.petty.etl.extractor;

import java.util.List;

import net.sf.json.JSONObject;

import org.bson.Document;

import com.petty.etl.factory.ExtractorFactory;
import com.petty.etl.factory.MongoDBFactory;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class EtlExtractor {

	public static List<JSONObject> extract(String data) {
		BaseExtractor be = ExtractorFactory.getExtractor(data);
		return be.extract(data);
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Long sTime = System.currentTimeMillis();   	    	
    	MongoDatabase src_db = MongoDBFactory.getClientInstance("192.168.1.21", 27017, "douban");
    	
    	//Document filter = new Document("url",new Document("$regex","/.*/group/topic/.*/"));
    	//Document filter = new Document("url",new Document("$regex","http://book.douban.com/subject/\\d+/$"));
    	FindIterable<Document> iterable = src_db.getCollection("html").find().limit(800);
    	//FindIterable<Document> iterable = src_db.getCollection("html").find(filter).limit(100);
    	iterable.forEach(new Block<Document>() {
    	    
    	    public void apply(final Document document) {
    	    	List<JSONObject> result = extract(document.toJson());
    	    	if (result != null) {
    	    		for(JSONObject r:result) {
    	    			if (r.toString().contains("subject")){
    	    				System.out.println(r.toString());
    	    			}
    	    		}
    	    	}
    	    }
    	});

    	Long eTime = System.currentTimeMillis();
        System.out.println( "time:"+(eTime-sTime)/1000 );
	}

}
