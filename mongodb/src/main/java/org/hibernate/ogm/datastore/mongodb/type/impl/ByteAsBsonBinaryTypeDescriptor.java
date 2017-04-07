/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;

import javax.xml.bind.DatatypeConverter;

import org.bson.types.Binary;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;

/**
 * @author Sergey Chernolyas &amp;sergey.chernolyas@gmail.com&amp;
 */
public class ByteAsBsonBinaryTypeDescriptor extends AbstractTypeDescriptor<Byte> {

	public static final ByteAsBsonBinaryTypeDescriptor INSTANCE = new ByteAsBsonBinaryTypeDescriptor();

	public ByteAsBsonBinaryTypeDescriptor() {
		super( Byte.class );
	}

	@Override
	public String toString(Byte binary) {
		return DatatypeConverter.printByte( binary );
	}

	@Override
	public Byte fromString(String hexStr) {
		return DatatypeConverter.parseByte( hexStr );
	}

	@Override
	public <X> X unwrap(Byte value, Class<X> type, WrapperOptions options) {
		return (X) new Binary( new byte[]{ value } );
	}

	@Override
	public <X> Byte wrap(X value, WrapperOptions options) {
		if ( value == null ) {
			return null;
		}
		Byte oneByte = (byte) 0;
		if ( value.getClass() == Binary.class ) {
			oneByte = ( (Binary) value ).getData()[0];
		}
		return oneByte;
	}
}
