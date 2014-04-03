package io.github.innotech.hydra.mongo;

import io.github.innotech.hydra.client.HydraClient;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import javax.ws.rs.core.UriBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * The main propose of this class is manage a mongodb connection using hydra as
 * a source of the mongo server ip. Every request check if the list of servers
 * has changes, if this happens create a new connection to mongo database with
 * the new server address list. The previous connection remains open until the
 * next hydra server change in order to fulfills the running connection
 * requests.
 * 
 */
public class HydratedMongoDB {

	private DB mongoDatabase;

	private String applicationName;

	private String databaseName;

	private HydraClient hydraClient;

	private LinkedHashSet<String> servers = new LinkedHashSet<String>();

	private MongoClient mongoClient;

	private ReadPreference readPreference = ReadPreference.primary();

	private WriteConcern writeConcern = WriteConcern.NORMAL;

	private ReentrantLock mutex = new ReentrantLock();

	// The connection with mongo remains open until the next hydra server change
	// for not destroy the running request
	// with mongo in this connection.
	private MongoClient staleMongoClient;

	public HydratedMongoDB(String applicationName, String databaseName, HydraClient hydraClient) {
		this.hydraClient = hydraClient;
		this.applicationName = applicationName;
		this.databaseName = databaseName;
	}

	/**
	 * Method that initialize the mongo connection. The call of this method not
	 * is mandatory. If not call the first call to getColection initialize.
	 */
	public void initMongoConnection() {
		if (mutex.tryLock()) {
			try {
				servers = hydraClient.get(applicationName);
				createNewMongoClient();
			} finally {
				mutex.unlock();
			}
		}
	}

	/**
	 * Return the mongo collection with the given name. Initialize the
	 * connection if not exist.
	 */
	public DBCollection getCollection(String collectionName) {
		LinkedHashSet<String> newServers = hydraClient.get(applicationName);

		if (newServers.isEmpty()) {
			throw new IllegalStateException("Hydra not found any active mongo servers.");
		}

		try {
			mutex.lock();
			if (!servers.equals(newServers)) {
				swapMongoServers(newServers);
			}
		} finally {
			mutex.unlock();
		}

		return mongoDatabase.getCollection(collectionName);
	}

	private void swapMongoServers(LinkedHashSet<String> newServers) {
		MongoClient oldMongoClient = staleMongoClient;

		servers = newServers;
		staleMongoClient = mongoClient;

		createNewMongoClient();

		if (oldMongoClient != null) {
			oldMongoClient.close();
		}

		mongoDatabase = mongoClient.getDB(databaseName);
	}

	private void createNewMongoClient() {
		mongoClient = new MongoClient(createServerAddress(servers));
		mongoClient.setReadPreference(readPreference);
		mongoClient.setWriteConcern(writeConcern);
	}

	private List<ServerAddress> createServerAddress(Collection<String> servers) {
		List<ServerAddress> serverAdresses = new ArrayList<ServerAddress>();

		for (String server : servers) {
			try {
				URI uri = UriBuilder.fromPath(server).build();
				serverAdresses.add(new ServerAddress(uri.getHost(), uri.getPort()));
			} catch (UnknownHostException e) {
				throw new IllegalStateException();
			}
		}

		return serverAdresses;
	}

	public ReadPreference getReadPreference() {
		return readPreference;
	}

	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	public WriteConcern getWriteConcern() {
		return writeConcern;
	}

	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}
}
