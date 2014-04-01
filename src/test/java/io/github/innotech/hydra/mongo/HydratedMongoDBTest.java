package io.github.innotech.hydra.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import io.github.innotech.hydra.client.HydraClient;
import io.github.innotech.hydra.client.HydraClientFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import javax.ws.rs.core.UriBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HydratedMongoDB.class,HydraClientFactory.class,UriBuilder.class})
public class HydratedMongoDBTest {
	
	private static String COLLECTION_NAME = "Collection";
	
	@Mock
	private HydraClient hydraClient;

	private static String APPLICATION = "application";
	
	private static String MONGO_SERVER_URL = "http://locahost:8800";
	
	private static String OTHER_MONGO_SERVER_URL = "https://locahost:8100";

	private static String DATABASE_NAME = "nameOfDatabase";
	
	private static String HOST = "host";
	
	private static Integer PORT = 1;

	@Mock
	private UriBuilder uriBuilder;
	
	@Mock(name="collection")
	private DBCollection collection;
	
	@Mock(name="secondCollection")
	private DBCollection secondCollection;
	
	@Mock
	private URI uri;
		
	@InjectMocks
	private HydratedMongoDB hydratedMongoDB = new HydratedMongoDB(APPLICATION,DATABASE_NAME,hydraClient);
	
	@Test
	public void shouldReturnTheCollectionOfCurrentDB() throws Exception{
		recordUriBuilderStub(MONGO_SERVER_URL);
		recordHydraClientStub(MONGO_SERVER_URL);	
		
		ServerAddress serverAddress = mock(ServerAddress.class,"serverAddress");
		MongoClient mongoClient = recordMongoClientStub(serverAddress);		
		
		recordMongoCollectionStub(mongoClient, "mongoDatabase", collection);
		
		DBCollection collection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		assertNotNull("The returned collection must be not null",collection);
		assertEquals("The collections was the same returned by the database",collection,collection);
	}

	@Test
	public void shouldNotChangeTheDatabaseIfSeversNotChange() throws Exception{
		recordUriBuilderStub(MONGO_SERVER_URL);
		recordHydraClientStub(MONGO_SERVER_URL);	
		
		ServerAddress serverAddress = mock(ServerAddress.class,"serverAddress");
		MongoClient mongoClient = recordMongoClientStub(serverAddress);		
		
		recordMongoCollectionStub(mongoClient, "mongoDatabase", collection);
		
		DBCollection firstCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		DBCollection secondCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		assertEquals("The collection must be equals",firstCollection,secondCollection);
	}

	@Test
	public void shouldChangeTheDatabaseIfSeversChange() throws Exception{
		recordUriBuilderStub(MONGO_SERVER_URL);
		recordHydraClientStub(MONGO_SERVER_URL);	
		
		ServerAddress serverAddress = mock(ServerAddress.class,"serverAddress");
		MongoClient mongoClient = recordMongoClientStub(serverAddress);		
		
		recordMongoCollectionStub(mongoClient, "mongoDatabase", collection);
		
		DBCollection firstCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		recordUriBuilderStub(OTHER_MONGO_SERVER_URL);
		recordHydraClientStub(OTHER_MONGO_SERVER_URL);	
		
		ServerAddress otherServerAddress = mock(ServerAddress.class,"otherServerAddress");
		MongoClient ohterMongoClient = recordMongoClientStub(otherServerAddress);		
		
		recordMongoCollectionStub(ohterMongoClient, "otherMongoDatabase", secondCollection);
		
		DBCollection secondCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		assertNotEquals("The collection must not be equals",firstCollection,secondCollection);
		verify(mongoClient).close();
	}
	
	private void recordMongoCollectionStub(MongoClient mongoClient, String database, DBCollection collection) {
		DB mongoDatabase = mock(DB.class,database);
		when(mongoClient.getDB(DATABASE_NAME)).thenReturn(mongoDatabase);
		when(mongoDatabase.getCollection(COLLECTION_NAME)).thenReturn(collection);
	}
	
	private void recordHydraClientStub(String uri) {
		LinkedHashSet<String> servers = new LinkedHashSet<String>();
		servers.add(uri);
		when(hydraClient.get(APPLICATION)).thenReturn(servers);
	}
	
	private void recordUriBuilderStub(String mongoUrl) {
		PowerMockito.mockStatic(UriBuilder.class);
		when(UriBuilder.fromPath(mongoUrl)).thenReturn(uriBuilder);
		when(uriBuilder.build()).thenReturn(uri);
	
		when(uri.getHost()).thenReturn(HOST);
		when(uri.getPort()).thenReturn(PORT);
	}
	
	private MongoClient recordMongoClientStub(ServerAddress serverAddress) throws Exception {
		Collection<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
		serverAddresses.add(serverAddress);
		PowerMockito.whenNew(ServerAddress.class).withArguments(HOST,PORT).thenReturn(serverAddress);
		MongoClient mongoClient = mock(MongoClient.class,"mongoClient");
		PowerMockito.whenNew(MongoClient.class).withArguments(serverAddresses).thenReturn(mongoClient);
		return mongoClient;
	}
	
}
