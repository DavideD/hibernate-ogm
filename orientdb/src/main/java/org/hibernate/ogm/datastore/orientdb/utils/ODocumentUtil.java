/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for working with OrientDB's class {@link ODocument}
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class ODocumentUtil {

	/**
	 * Prepare Map with keys of complex names like 'field11.field12'.
	 * <p>
	 * Simple names put to Map as is. Complex names put to Map as : <code>
	 * Map&lt;String, Object&gt; map = new HashMap&lt;&gt;();
	 * map.put("field1",new HashMap&lt;String, Object&gt;());
	 * map.get("field1").put("field2", value);
	 * </code>
	 * </p>
	 *
	 * @param rootFieldName root field name
	 * @param document document
	 * @return Map that contains keys with complex names
	 */
	public static Map<String, Object> extractNamesTree(String rootFieldName, ODocument document) {
		Map<String, Object> map = new LinkedHashMap<>();
		for ( int i = 0; i < document.fields(); i++ ) {
			String fieldName = rootFieldName.concat( "." ).concat( document.fieldNames()[i] );
			Object fieldValue = document.fieldValues()[i];
			if ( fieldValue instanceof ODocument ) {
				map.putAll( extractNamesTree( fieldName, (ODocument) fieldValue ) );
			}
			else {
				map.put( fieldName, fieldValue );
			}
		}
		return map;
	}

	public static Map<String, Object> toMap(ODocument document) {
		Map<String, Object> allFields = new LinkedHashMap<>( 20 );
		allFields.putAll( document.toMap() );
		allFields.put( "@version", document.field( "@version", Integer.class ) );
		return allFields;
	}
}
