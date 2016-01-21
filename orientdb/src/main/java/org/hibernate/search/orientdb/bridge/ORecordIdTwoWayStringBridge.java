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
package org.hibernate.search.orientdb.bridge;

import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.search.bridge.TwoWayStringBridge;

/**
 *
 * @author chernolyassv
 */
public class ORecordIdTwoWayStringBridge implements TwoWayStringBridge {

    @Override
    public Object stringToObject(String string) {
        System.out.println("ORecordIdTwoWayStringBridge: "+string);
        return new ORecordId(string);
    }

    @Override
    public String objectToString(Object o) {
        ORecordId id = (ORecordId) o;
        System.out.println("ORecordIdTwoWayStringBridge: "+id);
        return id.toString();
    }
    
}
