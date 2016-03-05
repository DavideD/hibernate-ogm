/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
<<<<<<< HEAD
 * 
=======
 *
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.query.impl;

<<<<<<< HEAD
import java.util.Set;
import org.hibernate.engine.query.spi.ParamLocationRecognizer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
=======
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.hibernate.engine.query.spi.ParamLocationRecognizer;
import org.junit.Test;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class OrientDBParameterMetadataBuilderTest {

<<<<<<< HEAD
	public OrientDBParameterMetadataBuilderTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

=======
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	/**
	 * Test of parseQueryParameters method, of class OrientDBParameterMetadataBuilder.
	 */
	@Test
	public void testParseQueryParameters() {
		System.out.println( "parseQueryParameters" );
		ParamLocationRecognizer recognizer = new ParamLocationRecognizer();
		String nativeQuery = "select from Customer where name=:name";
		OrientDBParameterMetadataBuilder instance = new OrientDBParameterMetadataBuilder();
		instance.parseQueryParameters( nativeQuery, recognizer );
		Set<String> parameters = instance.buildParameterMetadata( nativeQuery ).getNamedParameterNames();
		System.out.println( "1.parameters:" + parameters );
		assertNotNull( "Parameters in null!!!!", parameters );
		assertFalse( "Parameters in query must be!", parameters.isEmpty() );
		System.out.println( "1.parameters:" + parameters.contains( "name" ) );
		assertTrue( "Parameter 'name' must be!", parameters.contains( "name" ) );

		nativeQuery = "select from #29:0";
		instance.parseQueryParameters( nativeQuery, recognizer );
		parameters = instance.buildParameterMetadata( nativeQuery ).getNamedParameterNames();
		System.out.println( "2.parameters:" + parameters );
		assertTrue( "Parameters in query must not be!", parameters.isEmpty() );

		nativeQuery = "select from Customer where name=:name2";
		instance.parseQueryParameters( nativeQuery, recognizer );
		parameters = instance.buildParameterMetadata( nativeQuery ).getNamedParameterNames();
		System.out.println( "3.parameters:" + parameters );
		assertFalse( "Parameters in query must not be!", parameters.isEmpty() );
		assertTrue( "Parameter 'name2' must be!", parameters.contains( "name2" ) );

	}

}
