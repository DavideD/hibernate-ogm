/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.hibernate.ogm.model.key.spi.EntityKey;

/**
 * @author cristhiank on 3/6/16 (calovi86 at gmail.com).
 */
public class OrientDBQueryHelper {

	public static <T> OSQLSynchQuery<T> createSelect(EntityKey key) {
		String[] columnNames = key.getColumnNames();
		Object[] columnValues = key.getColumnValues();
		StringBuilder query = new StringBuilder( "SELECT FROM " );
		if ( columnValues.length == 1 && columnValues[0] instanceof ORecordId ) {
			query.append( columnValues[0] );
		}
		else if ( columnValues.length > 0 ) {
			query.append( key.getTable() );
			query.append( " WHERE " );
			int i = 0;
			do {
				if ( i > 0 ) {
					query.append( " AND " );
				}
				query.append( columnNames[i] ).append( " = " );
				if ( isNumeric( columnValues[i] ) ) {
					query.append( columnValues[i] );
				}
				else {
					query.append( "\"" ).append( columnValues[i] ).append( "\"" );
				}
			} while ( ++i < columnNames.length );
		}
		return new OSQLSynchQuery<T>( query.toString() );
	}

	private static boolean isNumeric(Object columnValue) {
		boolean isNumeric;
		isNumeric = Number.class.isAssignableFrom( columnValue.getClass() );
		isNumeric &= columnValue instanceof Number;
		return isNumeric;
	}
}
