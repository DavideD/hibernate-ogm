/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.bolt.impl;

import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.ogm.cfg.spi.Hosts;
import org.hibernate.ogm.datastore.neo4j.BoltNeo4jDialect;
import org.hibernate.ogm.datastore.neo4j.bolt.dialect.impl.BoltNeo4jSequenceGenerator;
import org.hibernate.ogm.datastore.neo4j.bolt.transaction.impl.BoltNeo4jTransactionCoordinatorBuilder;
import org.hibernate.ogm.datastore.neo4j.Neo4jProperties;
import org.hibernate.ogm.datastore.neo4j.logging.impl.Log;
import org.hibernate.ogm.datastore.neo4j.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.neo4j.query.parsing.impl.Neo4jBasedQueryParserService;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.query.spi.QueryParserService;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * @author Davide D'Alto
 */
public class BoltNeo4jDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static final Log log = LoggerFactory.getLogger();

	private static final int DEFAULT_SEQUENCE_QUERY_CACHE_MAX_SIZE = 128;

	private Driver neo4jDriver;

	private BoltNeo4jConfiguration configuration;

	private BoltNeo4jSequenceGenerator sequenceGenerator;

	private Integer sequenceCacheMaxSize;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return BoltNeo4jDialect.class;
	}

	@Override
	public TransactionCoordinatorBuilder getTransactionCoordinatorBuilder(TransactionCoordinatorBuilder coordinatorBuilder) {
		return new BoltNeo4jTransactionCoordinatorBuilder( coordinatorBuilder, this );
	}

	@Override
	public Class<? extends SchemaDefiner> getSchemaDefinerType() {
		return BoltNeo4jSchemaDefiner.class;
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return Neo4jBasedQueryParserService.class;
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
	}

	@Override
	public void configure(Map configurationValues) {
		configuration = new BoltNeo4jConfiguration( new ConfigurationPropertyReader( configurationValues ) );
		sequenceCacheMaxSize = new ConfigurationPropertyReader( configurationValues )
				.property( Neo4jProperties.SEQUENCE_QUERY_CACHE_MAX_SIZE, int.class )
				.withDefault( DEFAULT_SEQUENCE_QUERY_CACHE_MAX_SIZE )
				.getValue();
	}

	@Override
	public void start() {
		if ( neo4jDriver == null ) {
			try {
				this.neo4jDriver = createNeo4jDriver( configuration );
				validateConnection( neo4jDriver );
				this.sequenceGenerator = new BoltNeo4jSequenceGenerator( neo4jDriver, sequenceCacheMaxSize );
			}
			catch (HibernateException e) {
				// Wrap HibernateException in a ServiceException to make the stack trace more friendly
				// Otherwise a generic unable to request service is thrown
				throw log.unableToStartDatastoreProvider( e );
			}
		}
	}

	private Driver createNeo4jDriver(BoltNeo4jConfiguration configuration) {
		String uri = getDatabaseIdentifier().getDatabaseUri();
		if ( configuration.isAuthenticationRequired() ) {
			AuthToken authToken = AuthTokens.basic( configuration.getUsername(), configuration.getPassword() );
			return GraphDatabase.driver( uri, authToken );
		}
		else {
			return GraphDatabase.driver( uri );
		}
	}

	private void validateConnection(Driver neo4jDriver) {
		try {
			neo4jDriver.session().close();
		}
		catch (ClientException e) {
			throw log.connectionFailed( getDatabaseIdentifier().getDatabaseUri(), e.neo4jErrorCode(), e.getMessage() );
		}
	}

	@Override
	public void stop() {
		if ( neo4jDriver != null ) {
			neo4jDriver.close();
			neo4jDriver = null;
		}
	}

	private BoltNeo4jDatabaseIdentifier getDatabaseIdentifier() {
		if ( !configuration.getHosts().isSingleHost() ) {
			log.doesNotSupportMultipleHosts( configuration.getHosts().toString() );
		}
		Hosts.HostAndPort hostAndPort = configuration.getHosts().getFirst();
		try {
			return new BoltNeo4jDatabaseIdentifier( hostAndPort.getHost(), hostAndPort.getPort(), configuration.getDatabaseName(), configuration.getUsername(),
					configuration.getPassword() );
		}
		catch (Exception e) {
			throw log.malformedDataBaseUrl( e, hostAndPort.getHost(), hostAndPort.getPort(), configuration.getDatabaseName() );
		}
	}

	public Driver getDriver() {
		return neo4jDriver;
	}

	public BoltNeo4jSequenceGenerator getSequenceGenerator() {
		return sequenceGenerator;
	}
}
