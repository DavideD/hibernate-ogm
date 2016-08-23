/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.test.dsl;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.fest.assertions.Fail;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

/**
 * Assertion methods to check the mapping of nodes and relationships in Neo4j.
 *
 * @author Davide D'Alto
 */
public class GraphAssertions {

	public static NodeForGraphAssertions node(String alias, String... labels) {
		return new NodeForGraphAssertions( alias, labels);
	}

	public static void assertThatExists(Driver driver, NodeForGraphAssertions node) throws Exception {
		List<Record> records = null;
		String nodeAsCypher = node.toCypher();
		try ( Session session = driver.session() ) {
			String query = "MATCH " + nodeAsCypher + " RETURN " + node.getAlias();
			Statement statement = new Statement( query, node.getParams() );
			StatementResult result = session.run( statement );
			records = result.list();
		}
		assertThat( records ).isNotEmpty().as( "Node ["  + node.getAlias() + "] not found, Looked for " + nodeAsCypher + " with parameters: " + node.getParams() );

		Node nodeFound = records.get( 0 ).get( node.getAlias() ).asNode();
		Iterable<String> propertyKeys = nodeFound.keys();
		List<String> unexpectedProperties = new ArrayList<String>();
		Set<String> expectedProperties =  node.getProperties().keySet();
		for ( Iterator<String> iterator = propertyKeys.iterator(); iterator.hasNext(); ) {
			String actual = iterator.next();
			if ( !expectedProperties.contains( actual ) ) {
				unexpectedProperties.add( actual );
			}
		}

		List<String> missingProperties = new ArrayList<String>();
		if ( expectedProperties != null ) {
			for ( String expected : expectedProperties ) {
				if ( !nodeFound.containsKey( expected ) ) {
					missingProperties.add( expected );
				}
			}
		}
		assertThat( unexpectedProperties ).as( "Unexpected properties for node [" + node.getAlias() + "]" ).isEmpty();
		assertThat( missingProperties ).as( "Missing properties for node [" + node.getAlias() + "]" ).isEmpty();

		assertThat( records ).hasSize( 1 );
	}

	public static void assertThatExists(GraphDatabaseService engine, NodeForGraphAssertions node) throws Exception {
		Transaction tx = engine.beginTx();
		try {
			String nodeAsCypher = node.toCypher();
			String query = "MATCH " + nodeAsCypher + " RETURN " + node.getAlias();

			ResourceIterator<Object> columnAs = engine.execute( query, node.getParams() ).columnAs( node.getAlias() );
			assertThat( columnAs.hasNext() ).as( "Node ["  + node.getAlias() + "] not found, Looked for " + nodeAsCypher + " with parameters: " + node.getParams() ).isTrue();

			PropertyContainer propertyContainer = (PropertyContainer) columnAs.next();
			Iterable<String> propertyKeys = propertyContainer.getPropertyKeys();
			List<String> unexpectedProperties = new ArrayList<String>();
			Set<String> expectedProperties =  node.getProperties().keySet();
			for ( Iterator<String> iterator = propertyKeys.iterator(); iterator.hasNext(); ) {
				String actual = iterator.next();
				if ( !expectedProperties.contains( actual ) ) {
					unexpectedProperties.add( actual );
				}
			}
			List<String> missingProperties = new ArrayList<String>();
			if ( expectedProperties != null ) {
				for ( String expected : expectedProperties ) {
					if ( !propertyContainer.hasProperty( expected ) ) {
						missingProperties.add( expected );
					}
				}
			}
			assertThat( unexpectedProperties ).as( "Unexpected properties for node [" + node.getAlias() + "]" ).isEmpty();
			assertThat( missingProperties ).as( "Missing properties for node [" + node.getAlias() + "]" ).isEmpty();
			if ( columnAs.hasNext() ) {
				Fail.fail( "Unexpected result returned: " + columnAs.next() );
			}
			tx.success();
		}
		finally {
			tx.close();
		}
	}

	public static void assertThatExists(Driver engine, RelationshipsChainForGraphAssertions relationship) throws Exception {
		String relationshipAsCypher = relationship.toCypher();
		NodeForGraphAssertions node = relationship.getStart();
		String query = "MATCH " + relationshipAsCypher + " RETURN " + node.getAlias();

		List<Node> nodes = new ArrayList<>();
		try ( Session session = engine.session() ) {
			StatementResult result = session.run( query, relationship.getParams() );
			while ( result.hasNext() ) {
				nodes.add( result.next().get( node.getAlias() ).asNode() );
			}
		}

		assertThat( nodes ).isNotEmpty().as( "Relationships not found, Looked for " + relationshipAsCypher + " with parameters: " + relationship.getParams() );
		assertThat( nodes ).hasSize( 1 );
	}

	public static void assertThatExists(GraphDatabaseService engine, RelationshipsChainForGraphAssertions relationship) throws Exception {
		Transaction tx = engine.beginTx();
		try {
			String relationshipAsCypher = relationship.toCypher();
			NodeForGraphAssertions node = relationship.getStart();
			String query = "MATCH " + relationshipAsCypher + " RETURN " + node.getAlias();
			ResourceIterator<Object> columnAs = engine.execute( query, relationship.getParams() ).columnAs( node.getAlias() );
			assertThat( columnAs.hasNext() ).as( "Relationships not found, Looked for " + relationshipAsCypher + " with parameters: " + relationship.getParams() ).isTrue();
			columnAs.next();
			if ( columnAs.hasNext() ) {
				Fail.fail( "Unexpected result returned: " + columnAs.next() );
			}
			tx.success();
		}
		finally {
			tx.close();
		}
	}
}
