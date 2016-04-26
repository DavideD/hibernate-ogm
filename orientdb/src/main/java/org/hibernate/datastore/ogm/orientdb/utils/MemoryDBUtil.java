/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.jboss.logging.Logger;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class MemoryDBUtil {

	private static final Logger LOG = Logger.getLogger( MemoryDBUtil.class.getName() );
	private static OrientGraphFactory factory;

	public static void dropInMemoryDb() {
		LOG.log( Logger.Level.WARN, "drop current database " );
		if ( getOrientGraphFactory().getDatabase().exists() ) {
			getOrientGraphFactory().getDatabase().drop();
			LOG.log( Logger.Level.WARN, "current database  droped!" );
			getOrientGraphFactory().getDatabase().close();
			LOG.log( Logger.Level.WARN, "current database  closed!" );
		}
		factory.drop();
		factory = null;
	};

	public static void cleanDbFactory(String url) {
	}

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

	public static OrientGraphFactory getOrientGraphFactory() {
		return factory;
	}
}
