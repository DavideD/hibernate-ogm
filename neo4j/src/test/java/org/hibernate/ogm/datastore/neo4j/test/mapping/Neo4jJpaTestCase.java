/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.test.mapping;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.datastore.neo4j.test.dsl.GraphAssertions.assertThatExists;

import java.util.Map;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.neo4j.Neo4jProperties;
import org.hibernate.ogm.datastore.neo4j.embedded.impl.EmbeddedNeo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.logging.impl.Log;
import org.hibernate.ogm.datastore.neo4j.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.neo4j.remote.impl.RemoteNeo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.test.dsl.NodeForGraphAssertions;
import org.hibernate.ogm.datastore.neo4j.test.dsl.RelationshipsChainForGraphAssertions;
import org.hibernate.ogm.datastore.neo4j.utils.Neo4jTestHelper;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.jpa.impl.OgmEntityManagerFactory;
import org.hibernate.ogm.utils.jpa.GetterPersistenceUnitInfo;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.After;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * Common methods to check the mapping of entities in Neo4j.
 *
 * @author Davide D'Alto
 */
public abstract class Neo4jJpaTestCase extends OgmJpaTestCase {

	private static final Log log = LoggerFactory.getLogger();

	@Override
	protected void configure(GetterPersistenceUnitInfo info) {
		info.getProperties().setProperty( Neo4jProperties.DATABASE_PATH, Neo4jTestHelper.dbLocation() );
	}

	@After
	public void deleteAll() throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		Neo4jTestHelper.deleteAllElements( datastoreProvider );
	}

	protected void assertNumberOfRelationships(int rel) throws Exception {
		assertThat( numberOfRelationships() ).as( "Unexpected number of relationships" ).isEqualTo( rel );
	}

	protected void assertNumberOfNodes(int nodes) throws Exception {
		assertThat( numberOfNodes() ).as( "Unexpected number of nodes" ).isEqualTo( nodes );
	}

	protected Long numberOfNodes() throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		if ( datastoreProvider instanceof RemoteNeo4jDatastoreProvider ) {
			return executeRemoteCountWithBolt( "MATCH (n) RETURN COUNT(*) as count" );
		}
		else {
			return executeCount( "MATCH (n) RETURN COUNT(*) as count" );
		}
	}

	protected Long numberOfRelationships() throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		if ( datastoreProvider instanceof RemoteNeo4jDatastoreProvider ) {
			return executeRemoteCountWithBolt( "MATCH (n) - [r] -> () RETURN COUNT(r) as count" );
		}
		else {
			return executeCount( "MATCH (n) - [r] -> () RETURN COUNT(r) as count" );
		}
	}

	protected void executeCypherQuery(String query, Map<String, Object> parameters) throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		if ( datastoreProvider instanceof EmbeddedNeo4jDatastoreProvider ) {
			EmbeddedNeo4jDatastoreProvider provider = (EmbeddedNeo4jDatastoreProvider) datastoreProvider;
			GraphDatabaseService engine = provider.getDatabase();
			try {
				engine.execute( query, parameters );
			}
			catch (QueryExecutionException qe) {
				throw log.nativeQueryException( qe.getStatusCode(), qe.getMessage(), qe );
			}
		}
		else if ( datastoreProvider instanceof RemoteNeo4jDatastoreProvider ) {
			RemoteNeo4jDatastoreProvider provider = (RemoteNeo4jDatastoreProvider) datastoreProvider;
			try ( Session session = provider.getDriver().session() ) {
				try {
					StatementResult run = session.run( query, parameters );

					// Throw exception in case of error in the query execution
					run.hasNext();
				}
				catch (ClientException e) {
					throw log.nativeQueryException( e.neo4jErrorCode(), e.getMessage(), null );
				}
			}
		}
	}

	protected GraphDatabaseService createExecutionEngine() {
		DatastoreProvider datastoreProvider = datastoreProvider();
		EmbeddedNeo4jDatastoreProvider provider = (EmbeddedNeo4jDatastoreProvider) datastoreProvider;
		return provider.getDatabase();
	}

	protected Driver createDriver() {
		DatastoreProvider datastoreProvider = datastoreProvider();
		RemoteNeo4jDatastoreProvider provider = (RemoteNeo4jDatastoreProvider) datastoreProvider;
		return provider.getDriver();
	}

	private DatastoreProvider datastoreProvider() {
		OgmEntityManagerFactory emFactory = ( (OgmEntityManagerFactory) getFactory() );
		if ( emFactory != null ) {
			SessionFactoryImplementor sessionFactory = emFactory.getSessionFactory();
			DatastoreProvider datastoreProvider = sessionFactory.getServiceRegistry().getService( DatastoreProvider.class );
			return datastoreProvider;
		}
		return null;
	}

	private Long executeCount(String queryString) throws Exception {
		GraphDatabaseService graphDb = createExecutionEngine();
		Transaction tx = graphDb.beginTx();
		Result result = graphDb.execute( queryString );
		ResourceIterator<Long> count = result.columnAs( "count" );
		try {
			tx.success();
			return count.next();
		}
		finally {
			try {
				count.close();
			}
			finally {
				tx.close();
			}
		}
	}

	private Long executeRemoteCountWithBolt(String queryString) throws Exception {
		RemoteNeo4jDatastoreProvider datastoreProvider = (RemoteNeo4jDatastoreProvider) datastoreProvider();
		org.neo4j.driver.v1.Statement statement = new org.neo4j.driver.v1.Statement( queryString );

		try ( Session session = datastoreProvider.getDriver().session() ) {
			StatementResult result = session.run( statement );
			return result.next().get( 0 ).asLong();
		}
	}

	protected void assertThatOnlyTheseNodesExist(NodeForGraphAssertions... nodes) throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		if ( datastoreProvider instanceof RemoteNeo4jDatastoreProvider ) {
			for ( NodeForGraphAssertions node : nodes ) {
				assertThatExists( createDriver(), node );
			}
		}
		else {
			for ( NodeForGraphAssertions node : nodes ) {
				assertThatExists( createExecutionEngine(), node );
			}
		}
		assertNumberOfNodes( nodes.length );
	}

	protected void assertThatOnlyTheseRelationshipsExist(RelationshipsChainForGraphAssertions... relationships) throws Exception {
		DatastoreProvider datastoreProvider = datastoreProvider();
		if ( datastoreProvider instanceof RemoteNeo4jDatastoreProvider ) {
			int expectedNumberOfRelationships = 0;
			for ( RelationshipsChainForGraphAssertions relationship : relationships ) {
				assertThatExists( createDriver(), relationship );
				expectedNumberOfRelationships += relationship.getSize();
			}
			assertNumberOfRelationships( expectedNumberOfRelationships );

		}
		else {
			int expectedNumberOfRelationships = 0;
			for ( RelationshipsChainForGraphAssertions relationship : relationships ) {
				assertThatExists( createExecutionEngine(), relationship );
				expectedNumberOfRelationships += relationship.getSize();
			}
			assertNumberOfRelationships( expectedNumberOfRelationships );
		}
	}
}
