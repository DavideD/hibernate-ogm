/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.hibernate.datastore.ogm.orientdb.OrientDBDialect;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.ogm.util.configurationreader.spi.PropertyReaderContext;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider
implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static boolean isInmemoryDB = false;
	private static Log log = LoggerFactory.getLogger();
	private static OrientGraphFactory factory;
	private ConfigurationPropertyReader propertyReader;
	private ServiceRegistryImplementor registry;
	private JtaPlatform jtaPlatform;
	private JndiService jndiService;

	private Connection connection;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		log.debug( "start" );
		try {
			PropertyReaderContext<String> jdbcUrlPropery = propertyReader.property( "javax.persistence.jdbc.url", String.class );
			if ( jdbcUrlPropery != null ) {
				String jdbcUrl = jdbcUrlPropery.getValue();
				log.warn( "jdbcUrl:" + jdbcUrl );
				Class.forName( propertyReader.property( "javax.persistence.jdbc.driver", String.class ).getValue() ).newInstance();
				Properties info = new Properties();
				info.put( "user", propertyReader.property( "javax.persistence.jdbc.user", String.class ).getValue() );
				info.put( "password", propertyReader.property( "javax.persistence.jdbc.password", String.class ).getValue() );

				createInMemoryDB( jdbcUrl );

				connection = DriverManager.getConnection( jdbcUrl, info );
				setDateFormats( connection );
			}

		}
		catch (Exception e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private void setDateFormats(Connection connection) {
		String[] queries = new String[]{ "ALTER DATABASE DATETIMEFORMAT \"" + OrientDBConstant.DATETIME_FORMAT + "\"",
				"ALTER DATABASE DATEFORMAT \"" + OrientDBConstant.DATE_FORMAT + "\"" };
		for ( String query : queries ) {
			try {
				connection.createStatement().execute( query );
			}
			catch (SQLException sqle) {
				throw log.cannotExecuteQuery( query, sqle );
			}
		}
	}

	private static void createInMemoryDB(String jdbcUrl) {
		String orientDbUrl = jdbcUrl.substring( "jdbc:orient:".length() );

		if ( orientDbUrl.startsWith( "memory" ) ) {
			isInmemoryDB = true;
			MemoryDBUtil.createDbFactory( orientDbUrl );
		}

	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void stop() {

		log.debug( "stop" );
		if ( MemoryDBUtil.getOrientGraphFactory() != null ) {
			if ( MemoryDBUtil.getOrientGraphFactory().exists() ) {
				MemoryDBUtil.getOrientGraphFactory().close();
				MemoryDBUtil.getOrientGraphFactory().drop();
			}
		}
	}

	@Override
	public void configure(Map cfg) {
		log.debug( "config map:" + cfg.toString() );
		propertyReader = new ConfigurationPropertyReader( cfg );

	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.registry = serviceRegistry;
		jtaPlatform = serviceRegistry.getService( JtaPlatform.class );
		jndiService = serviceRegistry.getService( JndiService.class );
	}

	@Override
	public Class<? extends SchemaDefiner> getSchemaDefinerType() {
<<<<<<< HEAD
		LOG.info( "getSchemaDefinerType" );
		return OrientDBSchemaDefiner.class;
		// return super.getSchemaDefinerType();
=======
		return OrientDBSchemaDefiner.class;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}

}
