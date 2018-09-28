/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.type;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class GridFS {

	private final InputStream inputStream;

	public GridFS(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public GridFS(byte[] bytes) {
		this.inputStream = new ByteArrayInputStream( bytes );
	}

	public InputStream getInputStream() {
		return inputStream;
	}
}
