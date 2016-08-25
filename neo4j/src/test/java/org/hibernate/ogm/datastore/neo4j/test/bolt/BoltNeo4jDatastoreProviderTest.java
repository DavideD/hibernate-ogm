/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.test.bolt;

import static org.fest.assertions.Assertions.assertThat;

import org.hibernate.ogm.datastore.neo4j.bolt.impl.BoltNeo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.utils.Neo4jTestHelper;
import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.hibernate.ogm.utils.SkippableTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

/**
 * @author Davide D'Alto
 */
@RunWith(SkippableTestRunner.class)
@SkipByGridDialect(value = { GridDialectType.NEO4J_EMBEDDED }, comment = "We need a remote server")
public class BoltNeo4jDatastoreProviderTest {

	private BoltNeo4jDatastoreProvider provider;

	@Before
	public void before() {
		provider = new BoltNeo4jDatastoreProvider();
		provider.configure( Neo4jTestHelper.getConfiguration() );
		provider.start();
	}

	@After
	public void after() {
		provider.stop();
	}

	@Test
	public void testConnectionViaDriver() throws Exception {
		try ( Driver driver = provider.getDriver() ) {
			try ( Session session = driver.session() ) {
				session.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );
				StatementResult result = session.run( "MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title" );
				Record next = result.next();

				assertThat( next.get( "title" ).asString() ).isEqualTo( "King" );
				assertThat( next.get( "name" ).asString() ).isEqualTo( "Arthur" );

				deleteAll( session );
			}
		}
	}

	@Test
	public void testConnectionViaDriverNode() throws Exception {
		try ( Driver driver = provider.getDriver() ) {
			try ( Session session = driver.session() ) {
				session.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );
				StatementResult result = session.run( "MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a" );
				Record record = result.next();

				assertThat( record.get( "a" ).asNode().get( "title" ).asString() ).isEqualTo( "King" );
				assertThat( record.get( "a" ).asNode().get( "name" ).asString() ).isEqualTo( "Arthur" );

				deleteAll( session );
			}
		}
	}

	@Test
	public void testTransaction() throws Exception {
		try ( Driver driver = provider.getDriver() ) {
			try ( Session session = driver.session() ) {
				Transaction transaction = session.beginTransaction();
				transaction.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );
				StatementResult result = transaction.run( "MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a" );
				Record record = result.next();

				assertThat( record.get( "a" ).asNode().get( "title" ).asString() ).isEqualTo( "King" );
				assertThat( record.get( "a" ).asNode().get( "name" ).asString() ).isEqualTo( "Arthur" );

				transaction.run( "MATCH (n) OPTIONAL MATCH (n) -[r]- () DELETE n, r" );
				transaction.success();
				transaction.close();
			}
		}
	}

	private void deleteAll(Session session) {
		session.run( "MATCH (n) OPTIONAL MATCH (n) -[r]- () DELETE n, r" );
	}
}
