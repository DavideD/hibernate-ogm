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

	public static Map<String, Object> toMap(ODocument document) {
		Map<String, Object> allFields = new LinkedHashMap<>( 20 );
		allFields.putAll( document.toMap() );
		allFields.put( "@version", document.getVersion() );
		if ( document.containsField( "version" ) ) {
			allFields.put( "version", document.getVersion() );
		}
		return allFields;
	}
}
