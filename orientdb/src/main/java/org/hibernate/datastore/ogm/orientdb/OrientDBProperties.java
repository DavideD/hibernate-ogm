/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import org.hibernate.ogm.cfg.OgmProperties;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBProperties implements OgmProperties {

	/**
	 * Format of datetime. Default value 'yyyy-MM-dd HH:mm:ss'
	 */
	public static final String DATETIME_FORMAT = "hibernate.ogm.orientdb.format.datetime";
	/**
	 * Format of date. Default value 'yyyy-MM-dd'
	 */
	public static final String DATE_FORMAT = "hibernate.ogm.orientdb.format.date";

	/**
	 * Type of database. Possible values: memory,remote,plocal
	 */
	public static final String DATEBASE_TYPE = "hibernate.ogm.orientdb.dbtype";

	private OrientDBProperties() {
	}

	public static enum DatabaseTypeEnum {
		memory, plocal, remote
	}
}
