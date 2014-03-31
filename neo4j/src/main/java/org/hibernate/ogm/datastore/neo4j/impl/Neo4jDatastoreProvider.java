/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.ogm.datastore.neo4j.Neo4jDialect;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.Neo4jSequenceGenerator;
import org.hibernate.ogm.datastore.neo4j.parser.impl.Neo4jBasedQueryParserService;
import org.hibernate.ogm.datastore.neo4j.spi.GraphDatabaseServiceFactory;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Provides access to the Neo4j system.
 *
 * @author Davide D'Alto
 */
public class Neo4jDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private GraphDatabaseService neo4jDb;

	private GraphDatabaseServiceFactory graphDbFactory;

	private ServiceRegistryImplementor registry;

	private Neo4jSequenceGenerator sequenceGenerator;

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.registry = serviceRegistry;
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return Neo4jBasedQueryParserService.class;
	}

	@Override
	public void configure(Map cfg) {
		graphDbFactory = new Neo4jGraphDatabaseServiceFactoryProvider().load( cfg, registry.getService( ClassLoaderService.class ) );
	}

	@Override
	public void stop() {
		neo4jDb.shutdown();
	}

	@Override
	public void start() {
		this.neo4jDb = graphDbFactory.create();
		this.sequenceGenerator = new Neo4jSequenceGenerator( neo4jDb );
		this.graphDbFactory = null;
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return Neo4jDialect.class;
	}

	public GraphDatabaseService getDataBase() {
		return neo4jDb;
	}

	public SchemaBuilder getSchemaBuilder() {
		return new SchemaBuilder();
	}

	public Neo4jSequenceGenerator getSequenceGenerator() {
		return this.sequenceGenerator;
	}

	public class SchemaBuilder {

		private final Map<String, Set<String>> sequences = new HashMap<String, Set<String>>();

		public SchemaBuilder addSequence(String generatorKey, String segmentValue) {
			if ( sequences.containsKey( generatorKey ) ) {
				sequences.get( generatorKey ).add( segmentValue );
			}
			else {
				Set<String> segments = new HashSet<String>();
				segments.add( segmentValue );
				sequences.put( generatorKey, segments );
			}
			return this;
		}

		public void update() {
			sequenceGenerator.createUniqueConstraint( sequences );
		}
	}
}
