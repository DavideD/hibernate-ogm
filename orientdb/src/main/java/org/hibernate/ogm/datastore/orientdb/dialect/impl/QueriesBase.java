/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides common functionality required for query creation.
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class QueriesBase {

	protected Map<String, Object> params(Object[] columnValues) {
		return params( columnValues, 0 );
	}

	protected Map<String, Object> params(Object[] columnValues, int offset) {
		Map<String, Object> params = new LinkedHashMap<>( columnValues.length );
		for ( int i = 0; i < columnValues.length; i++ ) {
			params.put( String.valueOf( offset + i ), columnValues[i] );
		}
		return params;
	}

}
