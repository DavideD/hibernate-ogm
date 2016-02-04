/*
* Hibernate OGM, Domain model persistence for NoSQL datastores
* 
* License: GNU Lesser General Public License (LGPL), version 2.1 or later
* See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
*/

package org.hibernate.datastore.ogm.orientdb.utils;

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKey;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class EntityKeyUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static Object findPrimaryKeyValue(EntityKey key) {
		Object dbKeyValue = null;
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.info( "EntityKey: columnName: " + columnName + " is primary key!" );
				dbKeyValue = columnValue;
			}
		}
		return dbKeyValue;
	}

	public static String findPrimaryKeyName(EntityKey key) {
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.info( "EntityKey: columnName: " + columnName + " is primary key!" );
				return columnName;
			}
		}
		return null;
	}

}
