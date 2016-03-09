/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author cristhiank on 3/8/16 (calovi86 at gmail.com).
 */
public class OrientTransientVertex extends OrientVertex {

	Map<String, Object> properties;

	public OrientTransientVertex() {
		properties = new HashMap<>();
	}

	@Override public void setProperty(String key, Object value) {
		properties.put( key, value );
	}

	@Override public <T> T getProperty(String key) {
		return (T) properties.get( key );
	}

	@Override public Set<String> getPropertyKeys() {
		return properties.keySet();
	}
}
