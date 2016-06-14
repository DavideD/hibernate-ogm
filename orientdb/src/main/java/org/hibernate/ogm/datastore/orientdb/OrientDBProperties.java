/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb;

import org.hibernate.ogm.cfg.OgmProperties;

/**
 * Own properties of OrientDB Database Provider
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
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
	 * Type of database.
	 *
	 * @see DatabaseTypeEnum
	 */
	public static final String DATEBASE_TYPE = "hibernate.ogm.orientdb.dbtype";

	private OrientDBProperties() {
	}

	/**
	 * Enumeration of database's types
	 *
	 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
	 */
	public static enum DatabaseTypeEnum {
		memory, plocal, remote
	}
}
