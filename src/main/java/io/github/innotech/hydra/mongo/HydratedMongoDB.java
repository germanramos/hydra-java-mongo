package io.github.innotech.hydra.mongo;

import io.github.innotech.hydra.client.HydraClient;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import javax.ws.rs.core.UriBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class HydratedMongoDB {
	
	private DB mongoDatabase;
	
	private String applicationName;
	
	private String databaseName;
	
	private HydraClient hydraClient;

	private LinkedHashSet<String> servers = new LinkedHashSet<String>();

	private MongoClient mongoClient;

	//The connection with mongo remains open until the next hydra server change for not destroy the running request 
	//with mongo in this connection.
	private MongoClient staleMongoClient;
	
	public HydratedMongoDB(String applicationName, String databaseName, HydraClient hydraClient) {
		this.hydraClient = hydraClient;
		this.applicationName = applicationName;
		this.databaseName = databaseName;
	}

	public DBCollection getCollection(String collectionName) {
		LinkedHashSet<String> newServers = hydraClient.get(applicationName);
		
		if (newServers.isEmpty()){
			throw new IllegalStateException("Hydra not found any active mongo servers.");
		}
		
		if (!servers.equals(newServers)){
			swapMongoServers(newServers);
		}
	
		return mongoDatabase.getCollection(collectionName);
	}

	private void swapMongoServers(LinkedHashSet<String> newServers) {
		MongoClient oldMongoClient = staleMongoClient;

		servers = newServers;
		staleMongoClient = mongoClient;
		mongoClient = new MongoClient(createServerAddress(servers));
		
		if (oldMongoClient != null){
			oldMongoClient.close();
		}
		
		mongoDatabase = mongoClient.getDB(databaseName);
	}
	
	private List<ServerAddress> createServerAddress(Collection<String> servers) {
		List<ServerAddress> serverAdresses = new ArrayList<ServerAddress>();
		
		for (String server : servers) {
			try {
				URI uri = UriBuilder.fromPath(server).build();
				serverAdresses.add(new ServerAddress(uri.getHost(),uri.getPort()));
			} catch (UnknownHostException e) {
				throw new IllegalStateException();
			}
		}
		
		return serverAdresses;
	}	
}
