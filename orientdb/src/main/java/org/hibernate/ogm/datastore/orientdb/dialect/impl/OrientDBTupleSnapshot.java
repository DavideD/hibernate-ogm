/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.spi.TupleSnapshot;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * Represents a tuple snapshot as loaded by the datastore.
 * <p>
 * This interface is to be implemented by dialects to avoid data duplication in memory (if possible), typically wrapping
 * a store-specific representation of the tuple data. This snapshot will never be modified by the Hibernate OGM engine.
 * <p>
 * Note that in the case of embeddables (e.g. composite ids), column names are given using dot notation, e.g.
 * "id.countryCode" or "address.city.zipCode". The column names of the physical JPA model will be used, as e.g. given
 * via {@code @Column} .
 * <p>
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBTupleSnapshot implements TupleSnapshot {

	private static final Log log = LoggerFactory.getLogger();
	private final ODocument document;

	
        public OrientDBTupleSnapshot(ODocument document) {
		this.document = document;
	}

	@Override
	public Object get(String targetColumnName) {
		log.debugf( "targetColumnName: %s", targetColumnName );
		Object value = null;
		if ( targetColumnName.equals( OrientDBConstant.SYSTEM_VERSION ) ) {
			if ( targetColumnName.equalsIgnoreCase(OrientDBConstant.SYSTEM_VERSION ) ) {
				value = document.getVersion();
			}
			else {
				value = 0;
			}
			log.debugf( "targetColumnName: %s, value: %d", targetColumnName, value );
		}
		else if ( targetColumnName.equals( "version" ) ) {
			if ( targetColumnName.equalsIgnoreCase(OrientDBConstant.SYSTEM_VERSION ) ) {
				value = document.getVersion();
			}
			else {
				value = 0;
			}
		}
		else if ( targetColumnName.startsWith( "_identifierMapper." ) ) {
			value = document.field( targetColumnName.substring( "_identifierMapper.".length() ) );
		}
		else {
			value = document.field(targetColumnName );
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return document.isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
                Set<String> set = new LinkedHashSet<>();
                set.addAll(Arrays.asList(document.fieldNames()));
		return set;
	}

	/**
	 * Whether this snapshot has been newly created (meaning it doesn't have an actual {@link ODocument} yet) or not. A
	 * node will be in the "new" state between the {@code createTuple()} call and the next {@code insertOrUpdateTuple()}
	 * call.
	 *
	 * @return true if the snapshot is new (not saved in database yet), otherwise false
	 */
	public boolean isNew() {
		return ( document == null || document.isEmpty() );
	}
}
