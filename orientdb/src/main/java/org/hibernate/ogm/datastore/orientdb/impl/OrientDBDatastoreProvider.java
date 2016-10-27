/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.impl;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties.DatabaseTypeEnum;
import org.hibernate.ogm.datastore.orientdb.connection.ConnectionHolder;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.transaction.impl.OrientDbTransactionCoordinatorBuilder;
import org.hibernate.ogm.datastore.orientdb.utils.FormatterUtil;
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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static final long serialVersionUID = 1L;
	private static Log log = LoggerFactory.getLogger();
	private ConnectionHolder connectionHolder;
	private ConfigurationPropertyReader propertyReader;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		try {
			OrientDBProperties.DatabaseTypeEnum databaseType = propertyReader
					.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
					.withDefault( OrientDBProperties.DatabaseTypeEnum.MEMORY ).getValue();

			String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
			String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
			Integer poolSize = propertyReader.property( OrientDBProperties.POOL_SIZE, Integer.class ).withDefault( 10 ).getValue();
			String orientDBUrl = prepareOrientDbUrl( databaseType );
			createDB( orientDBUrl, databaseType );

			connectionHolder = new ConnectionHolder( orientDBUrl, user, password, poolSize );

			FormatterUtil.setDateFormatter( createFormatter( propertyReader, OrientDBProperties.DATE_FORMAT, "yyyy-MM-dd" ) );
			FormatterUtil.setDateTimeFormatter( createFormatter( propertyReader, OrientDBProperties.DATETIME_FORMAT, "yyyy-MM-dd HH:mm:ss" ) );
		}
		catch (Exception e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private ThreadLocal<DateFormat> createFormatter(final ConfigurationPropertyReader propertyReader, final String property, final String defaultFormat) {
		return new ThreadLocal<DateFormat>() {
			@Override
			protected DateFormat initialValue() {
				return new SimpleDateFormat( propertyReader.property( property, String.class ).withDefault( defaultFormat ).getValue() );
			}
		};
	}

	private String prepareOrientDbUrl(OrientDBProperties.DatabaseTypeEnum databaseType) {
		String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
		StringBuilder orientDbUrl = new StringBuilder( 100 );
		orientDbUrl.append( databaseType.name().toLowerCase() );
		switch ( databaseType ) {
			case MEMORY:
				orientDbUrl.append( ":" ).append( database );
				break;
			case REMOTE:
				String host = propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
				orientDbUrl.append( ":" ).append( host ).append( "/" ).append( database );
				break;
			default:
				throw new UnsupportedOperationException( String.format( "Database type %s unsupported!", databaseType ) );
		}
		return orientDbUrl.toString();
	}

	private void createDB(String orientDbUrl, DatabaseTypeEnum databaseType) {
		if ( OrientDBProperties.DatabaseTypeEnum.MEMORY.equals( databaseType ) ) {
			String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
			ODatabaseDocumentTx db = new ODatabaseDocumentTx( "memory:/" + database );
			db.create();
		}
		else if ( OrientDBProperties.DatabaseTypeEnum.REMOTE.equals( databaseType ) ) {
			if ( propertyReader.property( OgmProperties.CREATE_DATABASE, Boolean.class ).withDefault( Boolean.FALSE ).getValue() ) {
				String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
				String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
				String host = propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
				String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
				log.debugf( "Try to create remote database in JDBC URL %s ", orientDbUrl );
				OServerAdmin serverAdmin = null;
				try {
					serverAdmin = new OServerAdmin( "remote:" + host );
					serverAdmin.connect( user, password );
					boolean isDbExists = serverAdmin.existsDatabase( database, "plocal" );
					log.infof( "Database %s esists? %s.", database, String.valueOf( isDbExists ) );
					if ( !isDbExists ) {
						log.infof( "Database %s not exists. Try to create it.", database );
						serverAdmin.createDatabase( database, "graph", OrientDBConstant.PLOCAL_STORAGE_TYPE );
					}
					else {
						log.infof( "Database %s already exists", database );
					}
					// open the database
					ODatabaseDocumentTx db = new ODatabaseDocumentTx( "remote:" + host + "/" + database );
					db.open( user, password );
				}
				catch (Exception ioe) {
					throw log.cannotCreateDatabase( database, ioe );
				}
				finally {
					if ( serverAdmin != null ) {
						serverAdmin.close( true );
					}
				}
			}

		}
	}

	public ODatabaseDocumentTx getConnection() {
		return connectionHolder.get();
	}

	public void closeConnection() {
		connectionHolder.remove();
	}

	@Override
	public void stop() {
		log.debug( "---stop---" );
	}

	@SuppressWarnings("rawtypes")
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
