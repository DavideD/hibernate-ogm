/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hibernate.ogm.datastore.mongodb.options.BinaryStorageType;

public class FieldsWithBinaryStorageOption {

	private final Class<?> entityClass;

	private Map<Field, BinaryStorageType> storages = new HashMap<>();

	public FieldsWithBinaryStorageOption(Class<?> entityClass) {
		this.entityClass = entityClass;
	}

	public void add(Field field, BinaryStorageType binaryStorageType) {
		storages.put( field, binaryStorageType );
	}

	public BinaryStorageType get(String fieldName) {
		for ( Entry<Field, BinaryStorageType> entry : storages.entrySet() ) {
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
