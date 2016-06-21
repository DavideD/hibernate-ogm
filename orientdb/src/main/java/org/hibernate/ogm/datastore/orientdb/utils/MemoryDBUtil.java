/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;

/**
 * The utility class allow to manage OrientDB in mode 'memory'
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class MemoryDBUtil {

	private static Log log = LoggerFactory.getLogger();
	private static OrientGraphFactory factory;

	/**
	 * drop current instance of database
	 */
	public static void dropInMemoryDb() {
		log.info( "drop current database " );
		if ( getOrientGraphFactory().getDatabase().exists() ) {
			getOrientGraphFactory().getDatabase().drop();
			log.warn( "current database  droped!" );
			getOrientGraphFactory().getDatabase().close();
			log.warn( "current database  closed!" );
		}
		factory.drop();
		factory = null;
	};

	/**
	 * create database factory
	 *
	 * @param url url of database
	 * @return created database
	 * @see ODatabaseDocumentTx
	 */

	public static ODatabaseDocumentTx createDbFactory(String url) {
		if ( factory != null ) {
			factory.drop();
			factory = null;
		}
		factory = new OrientGraphFactory( url, true );
		// see https://github.com/orientechnologies/orientdb/issues/5688
		factory.setStandardElementConstraints( false );
		factory.setUseLog( true );
		ODatabaseDocumentTx db = factory.getDatabase( true, true );
		return db;
	}

	/**
	 * get instance of database factory
	 *
	 * @return factory's instance
	 */
	public static OrientGraphFactory getOrientGraphFactory() {
		return factory;
	}
}
