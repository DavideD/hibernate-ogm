/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;

import org.bson.types.Binary;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.descriptor.impl.WrappedGridTypeDescriptor;
import org.hibernate.ogm.type.impl.AbstractGenericBasicType;

/**
 * Persists {@link Binary}s as is in MongoDB.
 *
 * @author Sergey Chernolyas &amp;sergey.chernolyas@gmail.com&amp;
 *
 */
public class ByteAsBsonBinaryGridType extends AbstractGenericBasicType<Byte> {

	public static final ByteAsBsonBinaryGridType INSTANCE = new ByteAsBsonBinaryGridType();

	public ByteAsBsonBinaryGridType() {
		super( WrappedGridTypeDescriptor.INSTANCE, ByteAsBsonBinaryTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "bytes_as_bson_binary";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}
