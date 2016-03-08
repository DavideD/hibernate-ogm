/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;

import java.util.Set;

/**
 * @author chernolyassv
 * @author cristhiank (calovi86@gmail.com)
 */
public class OrientDBTupleSnapshot implements TupleSnapshot {

	private final OrientVertex orientDbRecord;
	private final EntityKeyMetadata recordKeyMetadata;
	private SnapshotType snapshotType;

	public OrientDBTupleSnapshot(OrientVertex record, EntityKeyMetadata meta, SnapshotType type) {
		this.orientDbRecord = record;
		this.recordKeyMetadata = meta;
		this.snapshotType = type;
	}

	public SnapshotType getSnapshotType() {
		return snapshotType;
	}

	public void setSnapshotType(SnapshotType snapshotType) {
		this.snapshotType = snapshotType;
	}

	@Override
	public Object get(String column) {
		return getOrientDbRecord().getProperty( column );
	}

	@Override
	public boolean isEmpty() {
		return getOrientDbRecord().getIdentity().isNew();
	}

	@Override
	public Set<String> getColumnNames() {
		return getOrientDbRecord().getPropertyKeys();
	}

	public OrientVertex getOrientDbRecord() {
		return orientDbRecord;
	}

	public enum SnapshotType {
		INSERT, UPDATE
	}

}
