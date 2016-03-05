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
package org.hibernate.datastore.ogm.orientdb.type.descriptor.java;

<<<<<<< HEAD
import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.type.descriptor.WrapperOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
=======
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.orientechnologies.orient.core.id.ORecordId;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class ORecordIdTypeDescriptorTest {

<<<<<<< HEAD
	public ORecordIdTypeDescriptorTest() {
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
	 * Test of toString method, of class ORecordIdTypeDescriptor.
	 */
	@Test
	public void testToString() {
		System.out.println( "toString" );
		ORecordId id = new ORecordId( 30, 100L );
		ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
		String expResult = "#30:100";
		String result = instance.toString( id );
		assertEquals( expResult, result );
	}

	/**
	 * Test of fromString method, of class ORecordIdTypeDescriptor.
	 */
	@Test
	public void testFromString() {
		System.out.println( "fromString" );
		String string = "#30:100";
		ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
		ORecordId expResult = new ORecordId( 30, 100L );
		ORecordId result = instance.fromString( string );
		assertEquals( expResult, result );
	}

<<<<<<< HEAD
	/**
	 * Test of unwrap method, of class ORecordIdTypeDescriptor.
	 */
	// @Test
	public void testUnwrap() {
		System.out.println( "unwrap" );
		ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
		Object expResult = null;
		// Object result = instance.unwrap(null);
		// assertEquals(expResult, result);
		// TODO review the generated test code and remove the default call to fail.
		// fail("The test case is a prototype.");
	}

	/**
	 * Test of wrap method, of class ORecordIdTypeDescriptor.
	 */
	// @Test
	public void testWrap() {
		System.out.println( "wrap" );
		Object x = null;
		WrapperOptions wo = null;
		ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
		ORecordId expResult = null;
		ORecordId result = instance.wrap( x, wo );
		// assertEquals(expResult, result);
		// TODO review the generated test code and remove the default call to fail.
		// fail("The test case is a prototype.");
	}

=======
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
}
