/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author chernolyassv
 * @author cristhiank (calovi86@gmail.com)
 */
public class OrientDBTupleSnapshot implements TupleSnapshot {

    private final ODocument orientDbRecord;
    private final EntityKeyMetadata recordKeyMetadata;
    private SnapshotType snapshotType;

    public SnapshotType getSnapshotType() {
        return snapshotType;
    }

    public void setSnapshotType(SnapshotType snapshotType) {
        this.snapshotType = snapshotType;
    }

    public enum SnapshotType {
        INSERT, UPDATE
    }

    public OrientDBTupleSnapshot(ODocument record, EntityKeyMetadata meta, SnapshotType type) {
        this.orientDbRecord = record;
        this.recordKeyMetadata = meta;
        this.snapshotType = type;
    }

    @Override
    public Object get(String column) {
        return getOrientDbRecord().field(column);
    }

    @Override
    public boolean isEmpty() {
        return getOrientDbRecord().isEmpty();
    }

    @Override
    public Set<String> getColumnNames() {
        Set<String> columnNames = new HashSet<>();
        Collections.addAll(columnNames, getOrientDbRecord().fieldNames());
        return columnNames;
    }

    public ODocument getOrientDbRecord() {
        return orientDbRecord;
    }

}
