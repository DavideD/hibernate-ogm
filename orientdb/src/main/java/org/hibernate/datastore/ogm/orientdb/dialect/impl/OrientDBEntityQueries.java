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

import com.orientechnologies.orient.core.id.ORecordId;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;

/**
 *
 * @author chernolyassv
 */
public class OrientDBEntityQueries extends QueriesBase {

    private static Log LOG = LoggerFactory.getLogger();

    private final EntityKeyMetadata entityKeyMetadata;

    public OrientDBEntityQueries(EntityKeyMetadata entityKeyMetadata) {
        this.entityKeyMetadata = entityKeyMetadata;
        for (int i = 0; i < entityKeyMetadata.getColumnNames().length; i++) {
            String columnName = entityKeyMetadata.getColumnNames()[i];
            LOG.info("column number:" + i + "; column name:" + columnName);
        }

    }

    /**
     * Find the node corresponding to an entity.
     *
     * @param executionEngine the {@link GraphDatabaseService} used to run the
     * query
     * @param columnValues the values in
     * {@link org.hibernate.ogm.model.key.spi.EntityKey#getColumnValues()}
     * @return the corresponding node
     */
    public Map<String, Object> findEntity(Connection executionEngine, Object[] columnValues) throws SQLException {
        Map<String, Object> params = params(columnValues);
        Map<String, Object> dbValues = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            LOG.info("key: " + key + "; value:" + value + "; (class: " + value.getClass() + ")");
        }

        Statement stmt = executionEngine.createStatement();
        StringBuilder query = new StringBuilder("select from ");
        if (params.size() == 1 && params.get("0") instanceof ORecordId) {
            ORecordId rid = (ORecordId) params.get("0");
            query.append(rid);
        } else {
            query.append(entityKeyMetadata.getTable());
        }

        LOG.info("find entiry query: " + query.toString());

        ResultSet rs = stmt.executeQuery(query.toString());
        if (rs.next()) {
            ResultSetMetaData metadata = rs.getMetaData();
            dbValues.put("@rid", rs.getObject("@rid"));

            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                int dbFieldNo = i + 1;
                String dbColumnName = metadata.getColumnName(dbFieldNo);
                LOG.info(i + " dbColumnName " + dbColumnName);
                dbValues.put(dbColumnName, rs.getObject(dbColumnName));
            }
            LOG.info(" entiry values from db: " + dbValues);
        }

        //Result result = executionEngine.execute(findEntityQuery, params);
        return dbValues;
    }

    private String findColumnByName(String name) {
        String index = "-1";
        for (int i = 0; i < entityKeyMetadata.getColumnNames().length; i++) {
            String columnName = entityKeyMetadata.getColumnNames()[i];
            if (columnName.equals(name)) {
                index = String.valueOf(i);
                break;
            }

        }
        return index;
    }

    private String findColumnByNum(int num) {
        return !(num > entityKeyMetadata.getColumnNames().length - 1)
                ? entityKeyMetadata.getColumnNames()[num] : null;
    }

}
