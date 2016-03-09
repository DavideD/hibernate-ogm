/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.hibernate.ogm.backendtck.simpleentity.CRUDTest;
import org.junit.Before;

/**
 * @author Davide D'Alto
 */
public class OrientDBTest extends CRUDTest {

	private static final String TEST_DB_URL = "memory:/tmp/orientdb_ogm_test";

	/**
	 * Drop and create the test classes (tables) schema in OrientDB
	 */
	@Before
	public void intialize() {
		OrientGraph graph = new OrientGraph( TEST_DB_URL, false );
		if ( graph.getVertexType( "Hypothesis" ) == null ) {
			graph.createVertexType( "Hypothesis" );
		}
		else {
			graph.dropVertexType( "Hypothesis" );
			graph.createVertexType( "Hypothesis" );
		}
		if ( graph.getVertexType( "Helicopter" ) == null ) {
			graph.createVertexType( "Helicopter" );
		}
		else {
			graph.dropVertexType( "Helicopter" );
			graph.createVertexType( "Helicopter" );
		}
		graph.shutdown( true );
	}
}
