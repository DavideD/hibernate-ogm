/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.impl;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

/**
 * Provides all information required to connect to a Neo4j database using the Bolt protocol.
 *
 * @author Davide D'Alto;
 */
public class RemoteNeo4jDatabaseIdentifier {

	private static final String PROTOCOL = "bolt://";
	private static final String SLASH = "/";

	private final String host;
	private final int port;
	private final String databaseName;
	private final String userName;
	private final String password;

	private final String serverUri;
	private final String databaseUri;

	public RemoteNeo4jDatabaseIdentifier(String host, int port, String databaseName, String userName, String password) throws MalformedURLException, URISyntaxException {
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.userName = userName;
		this.password = password;

		this.serverUri = PROTOCOL + host + ":" + port;
		this.databaseUri = serverUri + SLASH + databaseName;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * The name of the database
	 *
	 * @return the name of the database
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Returns the URI of the Neo4j server, e.g. "http://localhost:5984".
	 *
	 * @return the URI of the Neo4j server
	 */
	public String getServerUri() {
		return serverUri;
	}

	/**
	 * Returns the URI of the database, e.g. "http://localhost:5984/mydb".
	 *
	 * @return the URI of the database
	 */
	public String getDatabaseUri() {
		return databaseUri;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	@Override
	public String toString() {
		return "RemoteNeo4jDatabaseIdentifier [host=" + host + ", port=" + port + ", databaseName=" + databaseName + ", userName=" + userName + ", password=***"
				+ ", serverUri=" + serverUri + ", databaseUri=" + databaseUri + "]";
	}
}
