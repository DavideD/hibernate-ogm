/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.hibernate.HibernateException;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.type.descriptor.impl.BasicGridBinder;
import org.hibernate.ogm.type.descriptor.impl.GridTypeDescriptor;
import org.hibernate.ogm.type.descriptor.impl.GridValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;

import com.mongodb.client.gridfs.GridFSDownloadStream;

/**
 * @author Davide D'Alto
 */
public abstract class AbstractGridFSGridTypeDescriptor implements GridTypeDescriptor {

	@Override
	public <X> GridValueBinder<X> getBinder(final JavaTypeDescriptor<X> javaTypeDescriptor) {
		return new BasicGridBinder<X>( javaTypeDescriptor, this ) {

			@Override
			protected void doBind(Tuple resultset, X value, String[] names, WrapperOptions options) {
				InputStream stream = javaTypeDescriptor.unwrap( value, InputStream.class, options );
				resultset.put( names[0], stream );
			}
		};
	}

	byte[] toByteArray(final GridFSDownloadStream gridfsStream) {
		try ( DataInputStream stream = new DataInputStream( gridfsStream ) ) {
			int size = Long.valueOf( gridfsStream.getGridFSFile().getLength() ).intValue();
			byte[] stringAsBytes = new byte[size];
			stream.readFully( stringAsBytes );
			return stringAsBytes;
		}
		catch (IOException e) {
			throw new HibernateException( e );
		}
	}

}
