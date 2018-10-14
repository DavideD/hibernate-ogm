/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;

import java.io.InputStream;

import org.bson.types.Binary;
import org.hibernate.ogm.datastore.mongodb.type.GridFS;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.type.descriptor.impl.BasicGridBinder;
import org.hibernate.ogm.type.descriptor.impl.GridTypeDescriptor;
import org.hibernate.ogm.type.descriptor.impl.GridValueBinder;
import org.hibernate.ogm.type.descriptor.impl.GridValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;

public class GridFSGridTypeDescriptor implements GridTypeDescriptor {

	public static final GridFSGridTypeDescriptor INSTANCE = new GridFSGridTypeDescriptor();

	@Override
	public <X> GridValueBinder<X> getBinder(JavaTypeDescriptor<X> javaTypeDescriptor) {
		return new BasicGridBinder<X>( javaTypeDescriptor, this ) {

			@Override
			protected void doBind(Tuple resultset, X value, String[] names, WrapperOptions options) {
				InputStream inputstream = javaTypeDescriptor.unwrap( value, InputStream.class, options );
				resultset.put( names[0], new GridFS( inputstream ) );
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
