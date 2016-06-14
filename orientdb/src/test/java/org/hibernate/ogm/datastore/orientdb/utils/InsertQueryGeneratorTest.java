/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;

import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */

public class InsertQueryGeneratorTest {

	private InsertQueryGenerator instance = new InsertQueryGenerator();

	/**
	 * Test of createJSON method, of class InsertQueryGenerator.
	 */
	@Test
	public void testCreateJSON() throws Exception {
		System.out.println( "createJSON" );
		JSONObject result = null;
		Map<String, Object> valuesMap = new LinkedHashMap<>();
		valuesMap.put( "field1", 1l );

		result = instance.createJSON( true, Collections.<String>emptySet(), valuesMap );
		assertEquals( result.get( "field1" ), 1l );

		valuesMap.put( "field2", new byte[]{ 1, 2, 3 } );
		result = instance.createJSON( true, Collections.<String>emptySet(), valuesMap );
		assertEquals( result.get( "field2" ), new String( Base64.encodeBase64( new byte[]{ 1, 2, 3 } ) ) );

		// using embedded fields
		valuesMap.put( "field3.embeddedField1", "f1" );
		valuesMap.put( "field3.embeddedField2", "f2" );
		result = instance.createJSON( true, Collections.<String>emptySet(), valuesMap );
		assertTrue( "Field 'field3' must exists!", result.containsKey( "field3" ) );
		assertEquals( result.get( "field3" ).getClass(), JSONObject.class );
		JSONObject embeddedFiled = (JSONObject) result.get( "field3" );
		assertTrue( "JSON must have key '@type'", embeddedFiled.containsKey( "@type" ) );
		assertTrue( "JSON must have key '@class'", embeddedFiled.containsKey( "@class" ) );
		assertEquals( "field3", embeddedFiled.get( "@class" ) );
		assertEquals( "f1", embeddedFiled.get( "embeddedField1" ) );
		assertEquals( "f2", embeddedFiled.get( "embeddedField2" ) );

		valuesMap.put( "field4.ef1l1.ef1l2", "f11" );
		valuesMap.put( "field4.ef1l1.ef2l2", "f12" );
		valuesMap.put( "field4.ef1l1.ef3l2", "f13" );

		valuesMap.put( "field4.ef2l1.ef1l2", "f21" );
		valuesMap.put( "field4.ef2l1.ef2l2", "f22" );
		valuesMap.put( "field5", "http://www.hibernate.org/" );
		result = instance.createJSON( true, Collections.<String>emptySet(), valuesMap );
		assertTrue( "Field 'field4' must exists!", result.containsKey( "field4" ) );
		embeddedFiled = (JSONObject) result.get( "field4" );
		assertTrue( "JSON must have key 'ef1l1'", embeddedFiled.containsKey( "ef1l1" ) );
		assertTrue( "JSON must have key 'ef2l1'", embeddedFiled.containsKey( "ef2l1" ) );
		embeddedFiled = (JSONObject) ( (JSONObject) result.get( "field4" ) ).get( "ef1l1" );
		assertTrue( "JSON must have key 'ef1l1.ef1l2'", embeddedFiled.containsKey( "ef1l2" ) );
		assertEquals( "f11", embeddedFiled.get( "ef1l2" ) );

		System.out.println( "toString:" + result.toString() );
		System.out.println( "toJSONString:" + result.toJSONString() );

	}

}
