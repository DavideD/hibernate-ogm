/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;
import org.bson.types.ObjectId;
import org.hibernate.ogm.datastore.mongodb.type.GridFS;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;

/**
 * Descriptor for persisting {@link ObjectId}s as is in MongoDB.
 *
 * @author Gunnar Morling
 */
public class GridFSJavaTypeDescriptor extends AbstractTypeDescriptor<GridFS> {

	public static final GridFSJavaTypeDescriptor INSTANCE = new GridFSJavaTypeDescriptor();

	public GridFSJavaTypeDescriptor() {
		super( GridFS.class );
	}

	@Override
	public String toString(GridFS value) {
		return value == null ? null : value.toString();
	}
	@Override
	public GridFS fromString(String string) {
		if ( string == null ) {
			return null;
		}
		return new GridFS( string.getBytes() );
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public <X> X unwrap(GridFS value, Class<X> type, WrapperOptions options) {
		if ( value == null ) {
			return null;
		}
		if ( GridFS.class.isAssignableFrom( type ) ) {
			return (X) value;
		}
		throw unknownUnwrap( type );
	}

	@Override
	public <X> GridFS wrap(X value, WrapperOptions options) {
		if ( value == null ) {
			return null;
		}
		if ( GridFS.class.isInstance( value ) ) {
			return (GridFS) value;
		}
		throw unknownWrap( value.getClass() );
	}
}
