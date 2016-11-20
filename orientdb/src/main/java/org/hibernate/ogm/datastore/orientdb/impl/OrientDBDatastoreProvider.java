/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.impl;

import org.hibernate.ogm.datastore.orientdb.schema.OrientDBDocumentSchemaDefiner;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties.StorageModeEnum;
import org.hibernate.ogm.datastore.orientdb.connection.DatabaseHolder;
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
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties.DatabaseTypeEnum;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static final long serialVersionUID = 1L;
	private static Log log = LoggerFactory.getLogger();
	private DatabaseHolder databaseHolder;
	private ConfigurationPropertyReader propertyReader;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		log.debug( "---start---" );
		try {
			StorageModeEnum storageMode = propertyReader
					.property( OrientDBProperties.STORAGE_MODE_TYPE, OrientDBProperties.StorageModeEnum.class )
					.withDefault( OrientDBProperties.StorageModeEnum.MEMORY ).getValue();
			DatabaseTypeEnum databaseType = propertyReader
					.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
					.withDefault( OrientDBProperties.DatabaseTypeEnum.DOCUMENT ).getValue();
			if ( DatabaseTypeEnum.GRAPH.equals( databaseType ) ) {
				throw new UnsupportedOperationException( "Graph API is not supported yet. Use Document API!" );
			}

			String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
			String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
			Integer poolSize = propertyReader.property( OrientDBProperties.POOL_SIZE, Integer.class ).withDefault( 10 ).getValue();
			String orientDBUrl = prepareOrientDbUrl( storageMode );
			createDB( orientDBUrl, storageMode, databaseType, poolSize );

			databaseHolder = new DatabaseHolder( orientDBUrl, user, password, poolSize );

			FormatterUtil.setDateFormatter( createFormatter( propertyReader, OrientDBProperties.DATE_FORMAT, OrientDBConstant.DEFAULT_DATE_FORMAT ) );
			FormatterUtil
			.setDateTimeFormatter( createFormatter( propertyReader, OrientDBProperties.DATETIME_FORMAT, OrientDBConstant.DEFAULT_DATETIME_FORMAT ) );
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

	private String prepareOrientDbUrl(OrientDBProperties.StorageModeEnum databaseType) {
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

	private void createDB(String orientDbUrl, StorageModeEnum storageMode, DatabaseTypeEnum databaseType, Integer poolSize) {
		log.debug( "---createDB---" );
		String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
		String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
		if ( OrientDBProperties.StorageModeEnum.MEMORY.equals( storageMode ) ) {
			try {
				OPartitionedDatabasePoolFactory factory = new OPartitionedDatabasePoolFactory( poolSize );
				OPartitionedDatabasePool pool = factory.get( orientDbUrl, user, password );
				pool.setAutoCreate( true );
				ODatabaseDocumentTx db = pool.acquire();
				log.debugf( "db.isClosed(): %b", db.isClosed() );
				log.debugf( "db.isActiveOnCurrentThread(): %b", db.isActiveOnCurrentThread() );
			}
			catch (Exception e) {
				log.error( "Database creation error!", e );
			}
		}
		else if ( OrientDBProperties.StorageModeEnum.REMOTE.equals( storageMode ) ) {
			if ( propertyReader.property( OgmProperties.CREATE_DATABASE, Boolean.class ).withDefault( Boolean.FALSE ).getValue() ) {
				String rootUser = propertyReader.property( OrientDBProperties.ROOT_USERNAME, String.class ).withDefault( "root" ).getValue();
				String rootPassword = propertyReader.property( OrientDBProperties.ROOT_PASSWORD, String.class ).withDefault( "root" ).getValue();
				String host = propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
				String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
				log.debugf( "Try to create remote database in JDBC URL %s ", orientDbUrl );
				OServerAdmin serverAdmin = null;
				try {
					serverAdmin = new OServerAdmin( "remote:" + host );
					serverAdmin.connect( rootUser, rootPassword );
					boolean isDbExists = serverAdmin.existsDatabase( database, OrientDBConstant.PLOCAL_STORAGE_TYPE );
					log.infof( "Database %s esists? %s.", database, String.valueOf( isDbExists ) );
					if ( !isDbExists ) {
						log.infof( "Database %s not exists. Try to create it.", database );
						serverAdmin.createDatabase( database, databaseType.name().toLowerCase(), OrientDBConstant.PLOCAL_STORAGE_TYPE );
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

	public ODatabaseDocumentTx getCurrentDatabase() {
		return databaseHolder.get();
	}

	public void closeCurrentDatabase() {
		databaseHolder.remove();
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
		DatabaseTypeEnum databaseType = propertyReader
				.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
				.withDefault( OrientDBProperties.DatabaseTypeEnum.DOCUMENT ).getValue();
		return OrientDBDocumentSchemaDefiner.class;
	}

	@Override
	public TransactionCoordinatorBuilder getTransactionCoordinatorBuilder(TransactionCoordinatorBuilder coordinatorBuilder) {
		return new OrientDbTransactionCoordinatorBuilder( coordinatorBuilder, this );
	}
}
