/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.type.impl;

import java.sql.Blob;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.descriptor.impl.ByteArrayMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.BlobTypeDescriptor;

/**
 * @author Nicolas Helleringer
 */
public class BlobGridType extends AbstractGenericBasicType<Blob> {

	public static final BlobGridType INSTANCE = new BlobGridType();

	public BlobGridType() {
		super( ByteArrayMappedGridTypeDescriptor.INSTANCE, BlobTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "byte";
	}

	@Override
	protected boolean registerUnderJavaType() {
		return true;
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}
