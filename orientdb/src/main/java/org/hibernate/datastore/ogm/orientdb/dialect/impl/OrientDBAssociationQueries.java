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

import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

/**
 *
 * @author chernolyassv
 */
public class OrientDBAssociationQueries extends QueriesBase {
    private static final Log log = LoggerFactory.make();

    private final EntityKeyMetadata ownerEntityKeyMetadata;
    private final AssociationKeyMetadata associationKeyMetadata;

    public OrientDBAssociationQueries(EntityKeyMetadata ownerEntityKeyMetadata, AssociationKeyMetadata associationKeyMetadata) {
        this.ownerEntityKeyMetadata = ownerEntityKeyMetadata;
        this.associationKeyMetadata = associationKeyMetadata;
    }
    

}
