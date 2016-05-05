/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class ConnectionHolder extends ThreadLocal<Connection> {

	private static Log log = LoggerFactory.getLogger();
	private final String jdbcUrl;
	private final Properties info;
	private final Map<Long, OrientJdbcConnection> CONNECTIONS = Collections
			.synchronizedMap( new HashMap<Long, OrientJdbcConnection>() );

	public ConnectionHolder(String jdbcUrl, Properties info) {
		this.jdbcUrl = jdbcUrl;
		this.info = info;
	}

	@Override
	public Connection get() {
		log.debugf( "get connection for thread %s", Thread.currentThread().getName() );
		if ( !CONNECTIONS.containsKey( Thread.currentThread().getId() ) ) {
			CONNECTIONS.put( Thread.currentThread().getId(), createConnectionForCurrentThread() );
		}
		Connection connection = CONNECTIONS.get( Thread.currentThread().getId() );
		try {
			if ( connection.isClosed() ) {
				log.debugf( "connection for thread %s is closed", Thread.currentThread().getName() );
				CONNECTIONS.put( Thread.currentThread().getId(), createConnectionForCurrentThread() );
			}
		}
		catch (SQLException | OException sqle) {
			log.error( "Cannot recreate connection", sqle );
		}
		return CONNECTIONS.get( Thread.currentThread().getId() );
	}

	@Override
	public void remove() {
		log.debugf( "remove connection for thread %s", Thread.currentThread().getName() );
		try {
			get().close();
		}
		catch (SQLException | OException sqle) {
			log.error( "Cannot close connection", sqle );
		}
		CONNECTIONS.remove( Thread.currentThread().getId() );
	}

	private OrientJdbcConnection createConnectionForCurrentThread() {
		OrientJdbcConnection connection = null;
		try {
			log.debugf( "create connection %s for thread %s", jdbcUrl, Thread.currentThread().getName() );
			Properties properties = new Properties();
			properties.setProperty( "db.usePool", "true" );
			connection = (OrientJdbcConnection) DriverManager.getConnection( jdbcUrl, info );
			connection.setAutoCommit( false );
		}
		catch (SQLException sqle) {
			throw log.cannotCreateConnection( sqle );
		}
		return (OrientJdbcConnection) connection;
	}

}
