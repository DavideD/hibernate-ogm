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
package org.hibernate.datastore.ogm.orientdb.type.descriptor.java;

import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;

/**
 *
 * @author chernolyassv
 */
public class ORecordIdTypeDescriptor extends AbstractTypeDescriptor<ORecordId> {

    public static final ORecordIdTypeDescriptor INSTANCE = new ORecordIdTypeDescriptor();

    public ORecordIdTypeDescriptor() {
        super(ORecordId.class);
    }

    @Override
    public String toString(ORecordId t) {
        return t.toString();
    }

    @Override
    public ORecordId fromString(String rid) {
        return new ORecordId(rid);
    }

    @Override
    public <X> X unwrap(ORecordId value, Class<X> type, WrapperOptions wo) {
        if (value == null) {
            return null;
        }
        if (ORecordId.class.isAssignableFrom(type)) {
            return (X) value;
        } else if (String.class.isAssignableFrom(type)) {
            return (X) value.toString();
        }
        throw new UnsupportedOperationException("Class" + type + "not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <X> ORecordId wrap(X value, WrapperOptions wo) {
        if (value == null) {
            return null;
        }
        if (String.class.isInstance(value)) {
            return new ORecordId(((String) value));
        } else if (ORecordId.class.isInstance(value)) {
            return (ORecordId) value;
        }
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
