/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;

/**
 * Created by cristhiank on 3/6/16.
 */
public final class OrientDBProperties implements DocumentStoreProperties {

	/**
	 * Configuration property to define which engine is the app using.
	 * Default: remote
	 * see http://orientdb.com/docs/2.1/Concepts.html#database-url
	 */
	public static final String ENGINE = "hibernate.ogm.orientdb.engine";

	private OrientDBProperties() {
	}
}
