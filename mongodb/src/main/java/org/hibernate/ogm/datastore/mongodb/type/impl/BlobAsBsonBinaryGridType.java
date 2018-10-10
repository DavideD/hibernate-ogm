/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;

import java.sql.Blob;

import org.bson.BsonBinary;
import org.bson.types.Binary;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.type.descriptor.impl.BasicGridBinder;
import org.hibernate.ogm.type.descriptor.impl.GridTypeDescriptor;
import org.hibernate.ogm.type.descriptor.impl.GridValueBinder;
import org.hibernate.ogm.type.descriptor.impl.GridValueExtractor;
import org.hibernate.ogm.type.impl.AbstractGenericBasicType;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BlobTypeDescriptor;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;

/**
 * Persists {@link Binary}s as is in MongoDB.
 *
 * @author Sergey Chernolyas &amp;sergey.chernolyas@gmail.com&amp;
 */
public class BlobAsBsonBinaryGridType extends AbstractGenericBasicType<Blob> {

	public static final BlobAsBsonBinaryGridType INSTANCE = new BlobAsBsonBinaryGridType();

	public BlobAsBsonBinaryGridType() {
		super( BlobAsInputStreamGridTypeDescriptor.INSTANCE, BlobTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "blob_as_stream";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}

	private static class BlobAsInputStreamGridTypeDescriptor implements GridTypeDescriptor {

		public static final BlobAsInputStreamGridTypeDescriptor INSTANCE = new BlobAsInputStreamGridTypeDescriptor();

		@Override
		public <X> GridValueBinder<X> getBinder(final JavaTypeDescriptor<X> javaTypeDescriptor) {
			return new BasicGridBinder<X>( javaTypeDescriptor, this ) {

				@Override
				protected void doBind(Tuple resultset, X value, String[] names, WrapperOptions options) {
					byte[] data = javaTypeDescriptor.unwrap( value, byte[].class, options );
					resultset.put( names[0], new BsonBinary( data ) );
				}
			};
		}

		@Override
		public <X> GridValueExtractor<X> getExtractor(final JavaTypeDescriptor<X> javaTypeDescriptor) {
			return new GridValueExtractor<X>() {

				@Override
				public X extract(Tuple resultset, String name, WrapperOptions options) {
					final Binary result = (Binary) resultset.get( name );
					if ( result == null ) {
						return null;
					}
					else {
						byte[] data = result.getData();
						return javaTypeDescriptor.wrap( data, options );
					}
				}

				@Override
				public X extract(Tuple resultset, String name) {
					throw new UnsupportedOperationException( "" );
				}
			};
		}

	}
}
