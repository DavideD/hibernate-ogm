/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.transaction.impl.OrientDbTransactionCoordinatorBuilder;
import org.hibernate.ogm.datastore.orientdb.utils.ConnectionHolder;
import org.hibernate.ogm.datastore.orientdb.utils.FormatterUtil;
import org.hibernate.ogm.datastore.orientdb.utils.MemoryDBUtil;
import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider
implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static Log log = LoggerFactory.getLogger();
	private ConnectionHolder connectionHolder;
	private ConfigurationPropertyReader propertyReader;
	private ServiceRegistryImplementor registry;
	private JtaPlatform jtaPlatform;
	private JndiService jndiService;
	private Properties info;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		log.debug( "---start---" );
		try {
			Class.forName( "com.orientechnologies.orient.jdbc.OrientJdbcDriver" ).newInstance();
			OrientDBProperties.DatabaseTypeEnum databaseType = propertyReader
					.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
					.withDefault( OrientDBProperties.DatabaseTypeEnum.memory ).getValue();
			String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
			String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
			StringBuilder jdbcUrl = new StringBuilder( 100 );
			jdbcUrl.append( "jdbc:orient:" ).append( databaseType );
			switch ( databaseType ) {
				case memory:
					jdbcUrl.append( ":" ).append( propertyReader.property( OgmProperties.DATABASE, String.class ).getValue() );
					break;
				case remote:
					jdbcUrl.append( ":" ).append( propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue() );
					jdbcUrl.append( "/" ).append( propertyReader.property( OgmProperties.DATABASE, String.class ).getValue() );
					break;
				default:
					throw new UnsupportedOperationException( String.format( "Database type %s unsupported!", databaseType ) );
			}
			log.infof( "jdbcUrl: %s", jdbcUrl );
			info = new Properties();
			info.put( "user", user );
			info.put( "password", password );
			createInMemoryDB( jdbcUrl.toString() );
			connectionHolder = new ConnectionHolder( jdbcUrl.toString(), info );

			FormatterUtil.setDateFormater( new ThreadLocal<DateFormat>() {

				@Override
				protected DateFormat initialValue() {
					SimpleDateFormat f = new SimpleDateFormat(
							propertyReader.property( OrientDBProperties.DATE_FORMAT, String.class ).withDefault( "yyyy-MM-dd" ).getValue() );
					return f;
				}

			} );
			FormatterUtil.setDateTimeFormater( new ThreadLocal<DateFormat>() {

				@Override
				protected DateFormat initialValue() {
					SimpleDateFormat f = new SimpleDateFormat(
							propertyReader.property( OrientDBProperties.DATETIME_FORMAT, String.class ).withDefault( "yyyy-MM-dd HH:mm:ss" ).getValue() );
					return f;
				}

			} );
		}
		catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedOperationException e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private void createInMemoryDB(String jdbcUrl) {

		OrientDBProperties.DatabaseTypeEnum databaseType = propertyReader
				.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
				.withDefault( OrientDBProperties.DatabaseTypeEnum.memory ).getValue();
		if ( databaseType.equals( OrientDBProperties.DatabaseTypeEnum.memory ) ) {
			if ( MemoryDBUtil.getOrientGraphFactory() != null ) {
				log.debugf( "getCreatedInstancesInPool: %d", MemoryDBUtil.getOrientGraphFactory().getCreatedInstancesInPool() );
			}
			ODatabaseDocumentTx db = MemoryDBUtil.createDbFactory( jdbcUrl.substring( jdbcUrl.indexOf( OrientDBProperties.DatabaseTypeEnum.memory.name() ) ) );
			log.debugf( "in-memory database exists: %b ", db.exists() );
			log.debugf( "in-memory database closed: %b ", db.isClosed() );
		}
	}

	public Connection getConnection() {
		return connectionHolder.get();
	}

	public void closeConnection() {
		connectionHolder.remove();
	}

	@Override
	public void stop() {
		log.debug( "---stop---" );
	}

	@Override
	public void configure(Map cfg) {
		log.debugf( "config map: %s", cfg.toString() );
		propertyReader = new ConfigurationPropertyReader( cfg );
	}

	public ConfigurationPropertyReader getPropertyReader() {
		return propertyReader;
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		log.debug( "injectServices" );
		this.registry = serviceRegistry;
		jtaPlatform = serviceRegistry.getService( JtaPlatform.class );
		jndiService = serviceRegistry.getService( JndiService.class );
	}

	@Override
	public Class<? extends SchemaDefiner> getSchemaDefinerType() {
		return OrientDBSchemaDefiner.class;
	}

	@Override
	public TransactionCoordinatorBuilder getTransactionCoordinatorBuilder(TransactionCoordinatorBuilder coordinatorBuilder) {
		return new OrientDbTransactionCoordinatorBuilder( coordinatorBuilder, this );
	}
}
