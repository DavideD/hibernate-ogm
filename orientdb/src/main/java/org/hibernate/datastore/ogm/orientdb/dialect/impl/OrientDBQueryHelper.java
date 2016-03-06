package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.hibernate.ogm.model.key.spi.EntityKey;

/**
 * @author cristhiank on 3/6/16 (calovi86 at gmail.com).
 */
public class OrientDBQueryHelper {
    public static <T> OSQLSynchQuery<T> createSelect(EntityKey key) {
        String[] columnNames = key.getColumnNames();
        Object[] columnValues = key.getColumnValues();
        StringBuilder query = new StringBuilder("SELECT FROM ");
        if (columnValues.length == 1 && columnValues[0] instanceof ORecordId) {
            query.append(columnValues[0]);
        } else if (columnValues.length > 0) {
            query.append(" WHERE ");
            int i = 0;
            do {
                if (i > 0) {
                    query.append(" AND ");
                }
                query.append(columnNames[i]).append("=").append(columnValues[i]);
            } while (i++ < columnNames.length);
        }
        return new OSQLSynchQuery<T>(query.toString());
    }
}
