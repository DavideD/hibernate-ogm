/*
 * Copyright (C) 2016 Hibernate.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;

/**
 *
 * @author chernolyassv
 */
public class OrientDBTupleSnapshot implements TupleSnapshot {

    private static Log LOG = LoggerFactory.getLogger();
    private final Map<String, Object> dbNameValueMap;

    private final Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata;
    private final Map<String, String> rolesByColumn;
    private final EntityKeyMetadata entityKeyMetadata;

    public OrientDBTupleSnapshot(Map<String, Object> dbNameValueMap, Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata,
            Map<String, String> rolesByColumn, EntityKeyMetadata entityKeyMetadata) {
        this.dbNameValueMap = dbNameValueMap;
        this.associatedEntityKeyMetadata = associatedEntityKeyMetadata;
        this.rolesByColumn = rolesByColumn;
        this.entityKeyMetadata = entityKeyMetadata;
        LOG.info("dbNameValueMap:"+dbNameValueMap);
    }

    @Override
    public Object get(String targetColumnName) {
        LOG.info("targetColumnName: " + targetColumnName);
        return dbNameValueMap.get(targetColumnName);
    }
    @Override
    public boolean isEmpty() {
        LOG.info("isEmpty");
        return dbNameValueMap.isEmpty();
    }

    @Override
    public Set<String> getColumnNames() {
        LOG.info("getColumnNames");        
        return dbNameValueMap.keySet();
    }

}
