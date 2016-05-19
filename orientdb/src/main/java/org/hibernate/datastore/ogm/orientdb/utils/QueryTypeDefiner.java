/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */

public class QueryTypeDefiner {

	public static enum QueryType {
		INSERT, UPDATE, ERROR;
	}

	public static QueryType define(boolean existsInDB, boolean isNewSnapshot) {
		QueryType type = QueryType.ERROR;

		if ( isNewSnapshot && !existsInDB ) {
			type = QueryType.INSERT;
		}
		else if ( !isNewSnapshot && existsInDB ) {
			type = QueryType.UPDATE;
		}
		return type;
	}

}
