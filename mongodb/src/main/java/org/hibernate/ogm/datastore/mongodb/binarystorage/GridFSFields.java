/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GridFSFields {

	private final Class<?> entityClass;

	private Map<Field, String> bucketNames = new HashMap<>();

	public GridFSFields(Class<?> entityClass) {
		this.entityClass = entityClass;
	}

	public void add(Field field, String binaryStorageType) {
		bucketNames.put( field, binaryStorageType );
	}

	public Set<Field> getFields() {
		if ( bucketNames.isEmpty() ) {
			return Collections.emptySet();
		}
		return bucketNames.keySet();
	}

	public String getBucketName(String fieldName) {
		for ( Entry<Field, String> entry : bucketNames.entrySet() ) {
			if ( entry.getKey().getName().equals( fieldName ) ) {
				return entry.getValue();
			}
		}
		return null;
	}

	public Class<?> getEntityClass() {
		return entityClass;
	}

}
