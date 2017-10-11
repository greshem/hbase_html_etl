package com.petty.etl.factory;


import java.util.HashMap;
import java.util.Map;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDBFactory {
	
	private static Map<String,MongoClient> mongo_client_map = new HashMap<String,MongoClient>();
	private static Map<String,MongoDatabase> mongo_database_map = new HashMap<String,MongoDatabase>();
	
	private MongoDBFactory(){
		
	}
	
	public static MongoDatabase getClientInstance(String server_address,Integer port,String database){
		if(server_address==null || port==null || database==null){
			return null;
		}
		String client_key = server_address + ":" +port;
		String database_key = client_key + database;
		MongoDatabase db = mongo_database_map.get(database_key);
		if(db!=null){
			return db;
		}
		MongoClient client = mongo_client_map.get(client_key);
		if(client==null){
			client = new MongoClient(server_address, port);
		}
		return client.getDatabase(database);
	}
	
}
