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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.map.impl.MapTupleSnapshot;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.model.spi.Tuple;

/**
 *
 * @author chernolyassv
 */
public class ResultSetTupleIterator implements ClosableIterator<Tuple> {

    private static final Log log = LoggerFactory.getLogger();

    private final ResultSet resultSet;

    public ResultSetTupleIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            log.info("3.hasNext. resultSet.isLast():"+resultSet.isLast());
            return !resultSet.isLast();
        } catch (SQLException e) {
            log.error("Error with ResultSet", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple next() {
        log.info("call next()");
        
        try {
            resultSet.next();
            return convert();
        } catch (SQLException e) {
            log.error("Error with ResultSet", e);
            throw new RuntimeException(e);
        }
        
    }

    protected Tuple convert() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        for (int i =0; i<resultSet.getMetaData().getColumnCount();i++) {
            int fieldNum = i+1;
            map.put(resultSet.getMetaData().getColumnName(fieldNum),
                    resultSet.getObject(fieldNum));
        }
        map.put("@rid",resultSet.getObject("@rid"));
        log.info("field map: "+map);
        return new Tuple(new MapTupleSnapshot(map));
    }

    @Override
    public void remove() {
        try {
            resultSet.deleteRow();
        } catch (SQLException e) {
            log.error("Error with ResultSet", e);
        }
    }

    @Override
    public void close() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            log.error("Error with ResultSet", e);
        }
    }

}
