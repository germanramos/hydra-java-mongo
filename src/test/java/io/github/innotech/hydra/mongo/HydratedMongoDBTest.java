package io.github.innotech.hydra.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
		createFirstMongoClientMock();
		
		DB mongoDatabase = mock(DB.class,"mongoDatabase");
		when(mongoDatabase.getCollection(COLLECTION_NAME)).thenReturn(collection);
		
		DBCollection collection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		assertNotNull("The returned collection must be not null",collection);
		assertEquals("The collections was the same returned by the database",collection,collection);
	}
	
	@Test
	public void shouldChangeTheDatabaseIfSeversChange() throws Exception{
		createFirstMongoClientMock();
		
		DBCollection firstCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		createSecondMongoClientMock();
		DBCollection secondCollection = hydratedMongoDB.getCollection(COLLECTION_NAME);
		
		assertNotEquals("The collection must not be equals",firstCollection,secondCollection);
	}
	
	private void createSecondMongoClientMock() throws Exception {		
		ServerAddress otherServerAddress = mock(ServerAddress.class,"otherServerAddress");
		MongoClient otherMongoClient = mock(MongoClient.class,"otherMongoClient");
		DB otherMongoDatabase = mock(DB.class,"otherMongoDatabase");
		
		createHidraClient(OTHER_MONGO_SERVER_URL ,otherServerAddress, otherMongoClient);
		when(otherMongoClient.getDB(DATABASE_NAME)).thenReturn(otherMongoDatabase);
		when(otherMongoDatabase.getCollection(COLLECTION_NAME)).thenReturn(secondCollection);
	}

	private void createFirstMongoClientMock() throws Exception {
		ServerAddress serverAddress = mock(ServerAddress.class,"serverAddress");
		MongoClient mongoClient = mock(MongoClient.class,"mongoClient");
		DB mongoDatabase = mock(DB.class,"mongoDatabase");
		
		createHidraClient(MONGO_SERVER_URL ,serverAddress, mongoClient);
		when(mongoClient.getDB(DATABASE_NAME)).thenReturn(mongoDatabase);
		
		when(mongoDatabase.getCollection(COLLECTION_NAME)).thenReturn(collection);
	}

	private void createHidraClient(String mongoServerUrl,ServerAddress serverAddress,MongoClient mongoClient) throws Exception {
		PowerMockito.mockStatic(UriBuilder.class);
		when(UriBuilder.fromPath(mongoServerUrl)).thenReturn(uriBuilder);
		when(uriBuilder.build()).thenReturn(uri);
		
		when(uri.getHost()).thenReturn(HOST);
		when(uri.getPort()).thenReturn(PORT);
		
		LinkedHashSet<String> servers = new LinkedHashSet<String>();
		servers.add(mongoServerUrl);
			
		Collection<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
		serverAddresses.add(serverAddress);
		
		PowerMockito.whenNew(MongoClient.class).withArguments(serverAddresses).thenReturn(mongoClient);		
		PowerMockito.whenNew(ServerAddress.class).withArguments(HOST,PORT).thenReturn(serverAddress);
		
		when(hydraClient.get(APPLICATION)).thenReturn(servers);
	}
}
