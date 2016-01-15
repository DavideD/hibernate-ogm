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

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author chernolyassv
 */
public class QueriesBase {

    protected Map<String, Object> params(Object[] columnValues) {
        return params(columnValues, 0);
    }

    protected Map<String, Object> params(Object[] columnValues, int offset) {
        Map<String, Object> params = new HashMap<String, Object>(columnValues.length);
        for (int i = 0; i < columnValues.length; i++) {
            params.put(String.valueOf(offset + i), columnValues[i]);
        }
        return params;
    }

}
